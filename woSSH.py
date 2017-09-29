import paramiko
import os
import time
import threading
import json
import getpass

THREAD_LOCK = threading.Lock()

class WoSSH(object):
    def __init__(self, host, user, passwd, rsa):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.rsa= rsa
        self.ssh = None
        self.trans = None

    def ssh_conn(*args, **kwargs):
        def __decorator(func):
            def __wrapper(self, *args, **kwargs):
                if self.user and self.passwd:
                    try:
                        self.trans = paramiko.Transport((self.host, 22))
                        self.trans.connect(username=self.user, password=self.passwd)
                        self.ssh = paramiko.SSHClient()
                        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                        self.ssh._transport = self.trans
                    except Exception as e:
                        return {'statcode': '0',
                                'cmsg': 'SSH连接%s失败' % self.host,
                                'msg': str(e)}
                    else:
                        return func(self, *args, **kwargs)
                else:
                    try:
                        privatekeyfile = os.path.expanduser(self.rsa)
                        pkey = paramiko.RSAKey.from_private_key_file(privatekeyfile)
                        self.trans = paramiko.Transport((self.host, 22))
                        self.trans.connect(username=self.user, pkey=pkey)
                        self.ssh = paramiko.SSHClient()
                        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # 允许连接不在kown_hosts 文件中的主机
                        self.ssh._transport = self.trans
                    except Exception as e:
                        return {'statcode': '0',
                                'cmsg': 'SSH连接%s失败' % self.host,
                                'msg': str(e)}
                    else:
                        return func(self, *args, **kwargs)
            return __wrapper
        return __decorator

    @ssh_conn()
    def module_handler(self, module_path, module_argv, out_dict):
        return_map = {'args': {'module_path': module_path, 'module_argv': module_argv}}
        remote_module_path = '/tmp/%s-%s' % (os.path.basename(module_path), time.strftime('%Y%m%d%H%M%S', time.localtime()))

        try:
            sftp = self.ssh.open_sftp()
            sftp.put(module_path, remote_module_path)
        except Exception as e:
            return_map['statcode'] = '0'
            return_map['cmsg'] = '传送本地模块脚本%s至远程机器失败' % module_path
            return_map['msg'] = str(e)
        else:
            stdin, stdout, stderr = self.ssh.exec_command('chmod +x %s && %s %s' % (remote_module_path, remote_module_path, module_argv), get_pty=True)
            output, error = str(stdout.read(), encoding='utf-8'), str(stderr.read(), encoding='utf-8')
            if error and not output:
                return_map['statcode'] = '0'
                return_map['cmsg'] = '执行模块脚本%s错误' % module_path
                return_map['msg'] = str(error)
            else:
                if out_dict:
                    for line in output.split('\n'):
                        try:
                            json_data = json.loads(line)
                        except Exception as e:
                            pass
                        else:
                            if type(json_data) == dict:
                                return_map.update(json_data)
                                break
                    else:
                        return_map.update({'statcode':'0', 'msg':'the output is not in JSON format', 'cmsg':'没有json字典类型数据', 'data':output})
                else:
                    return_map['statcode'] = '1'
                    return_map['cmsg'] = '执行模块脚本%s成功' % module_path
                    return_map['msg'] = 'Successful'
                    return_map['data'] = output
            self.ssh.exec_command('rm %s' % (remote_module_path))
        return return_map


class ModuleThread(threading.Thread):
    def __init__(self, host, module_path, module_argv, user, passwd, rsa, out_dict):
        threading.Thread.__init__(self)
        self.host = host
        self.module_path = module_path
        self.module_argv = module_argv
        self.user = user
        self.passwd = passwd
        self.rsa = rsa
        self.out_dict= out_dict

    def run(self):
        wo = WoSSH(self.host, self.user, self.passwd, self.rsa)
        result = wo.module_handler(self.module_path, self.module_argv, self.out_dict)
        THREAD_LOCK.acquire()
        MODULE_RESULT_MAP[self.host] = result
        THREAD_LOCK.release()

MODULE_RESULT_MAP = {}
def module_runner(hosts=[], module_path='', module_argv='', user=getpass.getuser(), passwd=None, rsa='~/.ssh/id_rsa', out_dict=False):
    thread_list = []
    for host in hosts:
        thread_list.append(ModuleThread(host, module_path, module_argv, user or getpass.getuser(), passwd, rsa or '~/.ssh/id_rsa', out_dict))
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()
    return MODULE_RESULT_MAP


if __name__ == '__main__':
    output = module_runner(
        hosts = ['192.168.200.67',],
        module_path = '/room/works/py_script/get_device_info.py',   #本地模块脚本路径
        module_argv = '',   #传递给模块脚本的参数
        user = '',          #选填，默认当前执行用户
        passwd = '',        #选填，不指定密码，默认走rsa秘钥认证
        rsa = '',           #选填，默认'~/.ssh/id_rsa'， user+passwd全指定，优先使用user+passwd
        out_dict = True,    #模块输出的格式，是否输出json dict
    )

    for host, val in output.items():
        print(host)
        print(val)
