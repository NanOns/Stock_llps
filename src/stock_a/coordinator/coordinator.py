"""Stock A 本地协调器；模拟和真实运行分开，未知副作用一律要求恢复。"""
from __future__ import annotations
import argparse, datetime, hashlib, json, os, pathlib, re, subprocess, sys, time, uuid
from contextlib import contextmanager
import winjob

ROOT = pathlib.Path(r'G:\codex work\Stock A')
DELIVERY = pathlib.Path(r'G:\codex work\Stock A 交付\过程资料\协调器')
TMP = r'G:\codex_tmp'
VALIDATION_ROOT = pathlib.Path(r'G:\codex_tmp\test_temp\Stock A\coordinator_real_smoke')
MODELS = {'EXEC': ('gpt-6-luna', 'high'), 'AUDIT': ('gpt-6.1-terra', 'low'), 'STAGE': ('gpt-6.1-sol', 'high')}
TERMINAL = {'PASS', 'BLOCKED', 'RECOVERY_REQUIRED'}

def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def atomic(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with tmp.open('w', encoding='utf-8', newline='\n') as f:
            json.dump(obj, f, ensure_ascii=False, indent=2); f.write('\n'); f.flush(); os.fsync(f.fileno())
        for attempt in range(5):
            try: os.replace(tmp, path); break
            except PermissionError:
                if attempt==4: raise
                time.sleep(.05*(2**attempt))
    finally:
        if tmp.exists():
            try: tmp.unlink()
            except OSError: pass
def load(path):
    with path.open(encoding='utf-8-sig') as f: return json.load(f)
def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def receipt_context(path, limit=65536):
    raw=path.read_bytes()
    if len(raw)>limit: raise ValueError('审计回执超过独立上下文预算: '+str(path))
    return {'path':str(path.resolve()),'sha256':hashlib.sha256(raw).hexdigest(),'content':raw.decode('utf-8-sig')}
def clean(s):
    s = re.sub(r'(?i)(bearer\s+|api[_-]?key["\s:=]+|password["\s:=]+|token["\s:=]+)([^\s",}]+)', r'\1[REDACTED]', s)
    s = re.sub(r'(?i)(postgres(?:ql)?://[^:/\s]+:)[^@\s]+@', r'\1[REDACTED]@', s)
    return re.sub(r'\bsk-[A-Za-z0-9_-]{15,}\b','[REDACTED]',s)
def write_clean(path, data): path.write_text(clean(data), encoding='utf-8')
def normalize_command(command):
    """仅比较实际 Python 测试载荷，忽略 CLI/PowerShell 的外层转义与引号。"""
    value=command
    for _ in range(3):
        value=value.replace('\\\\','\\').replace('\\"','"').replace("\\'", "'")
    match=re.search(r'(?i)(?:^|\s)python\s+-c\s+(.*)$',value)
    if match: value=match.group(1)
    return re.sub(r'[\s\"\'`]', '', value).casefold()
def git(root, *args):
    p = subprocess.run(['git', '-c', 'core.quotePath=false', '-C', str(root), *args], capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=30)
    if p.returncode: raise RuntimeError('Git 查询失败: ' + clean(p.stderr))
    return p.stdout.strip()
@contextmanager
def lock(path):
    import msvcrt
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        f = path.open('r+b')
    except FileNotFoundError:
        try: f = path.open('x+b')
        except FileExistsError: f = path.open('r+b')
    try:
        try: msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError: raise RuntimeError('已有活动协调器或写者，拒绝重入')
        f.seek(0, os.SEEK_END)
        if f.tell() == 0:
            f.seek(0); f.write(b'0'); f.flush(); os.fsync(f.fileno())
        f.seek(0)
        yield
    finally:
        try: f.seek(0); msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError: pass
        f.close()

def save(home, state): state['updated_at']=now(); atomic(home/'state.json', state)
def validate_result(obj, task, role, base, mode, required_evidence, run_id=None, observed_commands=None, materialize=None):
    if not isinstance(obj,dict) or obj.get('task_id')!=task['task_id'] or obj.get('base_sha')!=base: raise ValueError('结果任务或基线不匹配')
    if run_id and obj.get('run_id')!=run_id: raise ValueError('结果 run_id 不匹配')
    if obj.get('verdict') not in ('PASS','NEEDS_REVISION','BLOCKED'): raise ValueError('结论不合法')
    if not isinstance(obj.get('findings'),list) or not isinstance(obj.get('evidence_paths'),list) or not isinstance(obj.get('next_action'),str): raise ValueError('结果字段缺失')
    if not isinstance(obj.get('result_sha'),str) or not isinstance(obj.get('tree_sha'),str): raise ValueError('结果 SHA 或 tree 缺失')
    if not isinstance(obj.get('test_commands'),list): raise ValueError('缺少本轮测试命令与结果')
    if obj.get('handoff') != ('WRITE_LEASE=RELEASED' if role=='EXEC' else 'AUDIT_HANDOFF=COMPLETE'): raise ValueError('交接标记缺失')
    if obj.get('scope_id')!=task['task_id']: raise ValueError('审计或执行范围不匹配')
    if role=='EXEC' and obj['verdict']=='NEEDS_REVISION': raise ValueError('执行不能产生审计返修结论')
    if obj['verdict']=='PASS' and not obj['evidence_paths']: raise ValueError('PASS 缺少证据')
    if role=='EXEC' and obj['verdict']=='PASS' and not obj['test_commands']: raise ValueError('执行 PASS 必须有本轮实际测试')
    observed_commands=observed_commands or []
    for item in obj['test_commands']:
        if not isinstance(item,dict) or not isinstance(item.get('command_sha256'),str) or item.get('exit_code')!=0:
            raise ValueError('测试命令回执无效或未通过')
        if mode!='SIMULATED' and not any(x.get('command_sha256')==item['command_sha256'] and x.get('exit_code')==0 for x in observed_commands):
            raise ValueError('测试命令未在本轮 Codex CLI 工具事件中观察到成功执行')
    if materialize: materialize(obj)
    if (mode in ('REAL_CLI','VALIDATION_CLI') and obj['verdict']!='BLOCKED') or (role=='STAGE' and obj['verdict']=='PASS'):
        expected=required_evidence
        actual={str(pathlib.Path(x).resolve()) for x in obj['evidence_paths']}
        if not expected or any(str(pathlib.Path(x).resolve()) not in actual for x in expected): raise ValueError('PASS 缺少任务指定证据')
    for path in obj['evidence_paths']:
        p=pathlib.Path(path)
        if not isinstance(path,str) or not p.is_absolute() or not p.is_file(): raise ValueError('证据路径无效')
        if mode=='REAL_CLI' and not (p.resolve().is_relative_to(ROOT.resolve()) or p.resolve().is_relative_to(DELIVERY.parent.resolve())): raise ValueError('证据超出授权根')
        if mode=='VALIDATION_CLI' and not p.resolve().is_relative_to(VALIDATION_ROOT.resolve()): raise ValueError('烟测证据越过隔离根')
    if obj['verdict']=='NEEDS_REVISION':
        required=('problem_id','file','scenario','expected','close_condition')
        if not obj['findings'] or any(not isinstance(x,dict) or any(not isinstance(x.get(k),str) or not x[k].strip() for k in required) for x in obj['findings']): raise ValueError('返修缺少定位、场景、独立预期或关闭条件')
    return obj

def materialize_evidence_outputs(obj, spec, role, record):
    """由 Coordinator 落盘 CLI 结构化回传的本轮证据，避免子沙箱 ACL 产物不可读。"""
    if spec['mode']=='SIMULATED': return
    expected={str(pathlib.Path(x).resolve()) for x in spec.get('expected_evidence',[])}
    outputs=obj.get('evidence_outputs',[])
    if not isinstance(outputs,list): raise ValueError('evidence_outputs 必须为数组')
    if role!='EXEC' or not expected:
        if outputs: raise ValueError('只允许执行角色回传本轮证据内容')
        return
    if obj.get('verdict')=='BLOCKED' and not outputs: return
    seen=set()
    for item in outputs:
        if not isinstance(item,dict) or set(item)!={'path','content'} or not isinstance(item['path'],str) or not isinstance(item['content'],str): raise ValueError('本轮证据内容格式无效')
        path=pathlib.Path(item['path']).resolve(); key=str(path)
        if key not in expected or key in seen: raise ValueError('证据内容路径不属于本轮预授权清单或重复')
        content=item['content']; data=content.encode('utf-8')
        if len(data)>65536: raise ValueError('单份控制证据超过 64 KiB')
        for identity in (spec['run_id'],spec['task_id'],spec['base_sha']):
            if identity not in content: raise ValueError('证据正文缺少本轮 run/task/base 身份')
        if path.exists():
            if path.read_bytes()!=data: raise ValueError('本轮证据目标已存在且内容不一致，拒绝覆盖')
            seen.add(key); record.append({'path':key,'size_bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),'materialized_by':'coordinator','recovered_existing':True}); continue
        path.parent.mkdir(parents=True,exist_ok=True)
        temp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
        try:
            with temp.open('xb') as f: f.write(data); f.flush(); os.fsync(f.fileno())
            os.replace(temp,path)
        finally:
            if temp.exists(): temp.unlink()
        seen.add(key); record.append({'path':key,'size_bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),'materialized_by':'coordinator'})
    missing=expected-seen
    if obj.get('verdict')!='BLOCKED' and missing: raise ValueError('CLI 未回传全部本轮预授权证据内容')

def classify_failure(stdout,stderr,root,base):
    message=(stderr+'\n'+stdout).lower()
    if re.search(r'quota|rate.limit|usage.limit|额度',message): return 'QUOTA',False
    if re.search(r'authentication|unauthorized|登录失效',message): return 'AUTH',False
    if re.search(r'permission denied|access denied|approval.required|拒绝访问|需要审批',message): return 'PERMISSION',False
    if not re.search(r'connection reset|connection refused|timed out|timeout|temporarily unavailable|http 50[0234]',message): return 'PROCESS_ERROR',False
    try:
        events=[json.loads(line) for line in stdout.splitlines() if line.strip()]
        if not any(e.get('type')=='thread.started' for e in events) or any(e.get('type') in ('item.started','turn.started') or e.get('type') not in ('thread.started','turn.started','turn.failed','error','item.completed') or (e.get('type')=='item.completed' and e.get('item',{}).get('type') not in ('error','agent_message')) for e in events): return 'TRANSIENT',False
        return 'TRANSIENT',git(root,'rev-parse','HEAD')==base and not git(root,'status','--porcelain')
    except (ValueError,AttributeError,RuntimeError): return 'TRANSIENT',False

def public_event(raw):
    """只将不含命令正文、模型正文和文件内容的事件摘要对外显示。"""
    try:
        event=json.loads(raw); kind=event.get('type','')
        item=event.get('item') or {}; item_type=item.get('type','')
        if kind=='thread.started':
            thread_id=event.get('thread_id')
            if not isinstance(thread_id,str) or len(thread_id)>80 or not re.fullmatch(r'[0-9a-fA-F-]{32,40}',thread_id): thread_id=None
            return kind,'会话已创建',thread_id
        if kind=='turn.started': return kind,'模型开始处理',None
        if kind=='item.started': return kind,('开始 '+item_type if item_type in ('command_execution','agent_message','error') else '开始处理步骤'),None
        if kind=='item.completed':
            if item_type=='agent_message': return kind,'模型已生成阶段回复',None
            if item_type=='error': return kind,'子任务返回错误',None
            return kind,('完成 '+item_type if item_type in ('command_execution','agent_message','error') else '处理步骤完成'),None
        if kind=='turn.completed': return kind,'本轮处理完成',None
        if kind=='turn.failed': return kind,'本轮处理失败',None
        if kind=='error': return kind,'CLI报告错误',None
    except (ValueError,AttributeError,TypeError):
        return None
    return None

def worker(home, run_id):
    run=home/'runs'/run_id; spec=load(run/'spec.json'); env=os.environ.copy(); env.update(TMP=TMP,TEMP=TMP,TMPDIR=TMP,PYTHONUTF8='1')
    role=spec['role']; mode=spec['mode']; marker={'run_id':run_id,'role':role,'started_at':now()}
    # 独立 worker 维持源码写锁；父进程崩溃后锁仍由 worker 持有。
    cm=lock(home/'writer.lock') if role=='EXEC' else lock(run/'audit.lock')
    with cm:
        atomic(run/'worker.json',dict(marker,pid=os.getpid()))
        activity={'run_id':run_id,'task_id':spec['task_id'],'role':role,'model':spec['model'],'status':'启动中','thread_id':None,'updated_at':now()}
        atomic(run/'activity.json',activity)
        import threading
        event_lock=threading.Lock(); telemetry={'events':{},'thread_id':None,'commands':[],'stdout_bytes':0,'stderr_bytes':0,'last_event_at':None}
        final_message=[None]; last_activity=[0.0]
        def on_output(stream,line):
            with event_lock:
                key=stream+'_bytes'; telemetry[key]+=len(line.encode('utf-8',errors='replace'))
                if stream=='stdout':
                    summary=public_event(line)
                    if summary:
                        kind,label,thread_id=summary; telemetry['events'][kind]=telemetry['events'].get(kind,0)+1
                        telemetry['last_event_at']=now()
                        if thread_id: telemetry['thread_id']=thread_id
                        activity.update(event=kind,status=label,thread_id=telemetry['thread_id'],updated_at=now())
                    try:
                        event=json.loads(line); item=event.get('item') or {}
                        if event.get('type')=='item.completed' and item.get('type')=='agent_message':
                            text=item.get('text','')
                            if isinstance(text,str) and len(text)<=262144: final_message[0]=text
                        if event.get('type')=='item.completed' and item.get('type')=='command_execution':
                            command=item.get('command',''); code=item.get('exit_code')
                            if isinstance(command,str) and isinstance(code,int) and len(telemetry['commands'])<100:
                                telemetry['commands'].append({'command_text':command[:16384],'command_sha256':hashlib.sha256(command.encode('utf-8')).hexdigest(),'exit_code':code,'status':item.get('status','')})
                    except (ValueError,AttributeError,TypeError): pass
                else:
                    telemetry['events']['stderr_lines']=telemetry['events'].get('stderr_lines',0)+1
                activity.update(stats={'events':sum(v for k,v in telemetry['events'].items() if k!='stderr_lines'),'stdout_bytes':telemetry['stdout_bytes'],'stderr_bytes':telemetry['stderr_bytes']},updated_at=now())
                current=time.monotonic()
                if current-last_activity[0]>=.25:
                    atomic(run/'activity.json',activity); last_activity[0]=current
        try:
            if mode=='SIMULATED':
                proc=winjob.run([sys.executable, str(home/'fake_cli.py'), str(run/'spec.json')],env=env,timeout=60,cwd=spec['root'],on_output=on_output)
            else:
                if role=='EXEC' and not spec.get('live_execution_authorized'): raise RuntimeError('真实业务写入尚未完成接管验收')
                args=[spec['codex'],'exec','--sandbox', 'workspace-write' if role=='EXEC' else 'read-only','--cd',spec['root'], '--model',spec['model'],'--config','model_reasoning_effort="'+spec['effort']+'"','--config','project_doc_max_bytes=131072','--json','--output-schema',str(run/'schema.json'),'-']
                if role=='EXEC':
                    for evidence in spec.get('expected_evidence',[]):
                        target=pathlib.Path(evidence).resolve()
                        if mode!='SIMULATED':
                            workspace=pathlib.Path(spec['root']).resolve()
                            allowed_workspace=ROOT.resolve() if mode=='REAL_CLI' else VALIDATION_ROOT.resolve()
                            allowed_external=DELIVERY.parent.resolve() if mode=='REAL_CLI' else VALIDATION_ROOT.resolve()
                            if not (target.is_relative_to(workspace) or target.is_relative_to(allowed_workspace) or target.is_relative_to(allowed_external)): raise ValueError('本轮证据输出越过工作区/验证/交付根')
                        target.parent.mkdir(parents=True,exist_ok=True)
                prompt=spec['prompt']
                proc=winjob.run(args,input_text=prompt,env=env,timeout=1800,cwd=spec['root'],on_output=on_output)
            telemetry.update(getattr(proc,'output_stats',{}))
            if telemetry.get('thread_id'):
                activity['thread_id']=telemetry['thread_id']
            if proc.returncode==0:
                if mode=='SIMULATED': candidate=proc.stdout
                else:
                    if final_message[0] is None: raise ValueError('CLI 未返回结构化最终结果')
                    candidate=final_message[0]
                if clean(candidate)!=candidate: raise ValueError('最终结果可能含凭据，拒绝归档原文')
                obj=json.loads(candidate)
                if obj.get('run_id')!=run_id: raise ValueError('模型结果未回传本轮 run_id')
                claimed=obj.get('test_commands',[])
                normalized=[]
                for item in claimed:
                    if not isinstance(item,dict) or not isinstance(item.get('command'),str) or item.get('result')!='PASS': raise ValueError('测试命令声明格式不合法或未通过')
                    needle=normalize_command(item['command'].strip())
                    if mode=='SIMULATED':
                        normalized.append({'command_sha256':hashlib.sha256(needle.encode('utf-8')).hexdigest(),'exit_code':0})
                    else:
                        match=next((x for x in telemetry['commands'] if x['exit_code']==0 and needle and needle in normalize_command(x.get('command_text',''))),None)
                        if not match: raise ValueError('声明的测试命令没有与本轮成功 CLI 工具事件对应')
                        normalized.append({'command_sha256':match['command_sha256'],'exit_code':match['exit_code']})
                obj['test_commands']=normalized
                telemetry['commands']=[{k:v for k,v in x.items() if k!='command_text'} for x in telemetry['commands']]
                atomic(run/'telemetry.json',dict(marker,finished_at=now(),**telemetry))
                atomic(run/'last.json',obj)
            reason,safe_retry=classify_failure(proc.stdout,proc.stderr,pathlib.Path(spec['root']),spec['base_sha']) if proc.returncode and mode!='SIMULATED' else (('NONE',False) if proc.returncode==0 else ('PROCESS_ERROR',mode=='SIMULATED' and proc.returncode==75))
            if mode=='SIMULATED' and proc.returncode and 'quota' in proc.stderr.lower(): reason='QUOTA'
            atomic(run/'exit.json',dict(marker,exit_code=proc.returncode,finished_at=now(),safe_retry=safe_retry,tree_stopped=True,stop_reason=reason))
        except Exception as exc:
            atomic(run/'telemetry.json',dict(marker,finished_at=now(),error_type=type(exc).__name__,events=telemetry.get('events',{}),thread_id=telemetry.get('thread_id'),stdout_bytes=telemetry.get('stdout_bytes',0),stderr_bytes=telemetry.get('stderr_bytes',0)))
            atomic(run/'exit.json',dict(marker,exit_code=-1,finished_at=now(),safe_retry=isinstance(exc,FileNotFoundError),tree_stopped=False,stop_reason='WORKER_ERROR',error_type=type(exc).__name__))

def prompt_for(task, role, base, root, run_id):
    card_instruction=('先完整读取任务卡 '+task['card']+'。' if not task.get('repair_card_content') else 'Coordinator 已生成并校验本轮返修卡正文，见下方“返修任务卡正文”；无需访问 Coordinator 状态目录。')
    return ('你是 Stock A 有界'+role+'上下文。先完整读取 '+str(root/'AGENTS.md')+'、'+str(root/'PROJECT_SPEC.md')+'。'+card_instruction
            +'不得创建后继，不得修改旧V4、来源、生产PG、正式Head或部署，不得绕过审批。'
            +('执行方按任务卡及本任务获准动作完成实现、测试和证据，写入只能位于获准范围；只输出JSON。' if role=='EXEC' else '审计只读，不执行测试、诊断或构建；只输出JSON。')
            +'Windows工具可能从其他当前目录启动；所有文件使用绝对路径，所有Git命令显式使用 git -C "'+str(root)+'"，不得假设当前目录。先实际用 git -C 核对 HEAD 和 HEAD^{tree}。'
            +'任务ID '+task['task_id']+'，本轮run_id '+run_id+'，基线 '+base+'。固定输出 task_id,run_id,base_sha,result_sha,tree_sha,scope_id,handoff,verdict,findings,evidence_paths,next_action,test_commands,evidence_outputs。'
            +'字段另含 tree_sha、scope_id（填任务ID）、handoff（本角色必须填 '+('WRITE_LEASE=RELEASED' if role=='EXEC' else 'AUDIT_HANDOFF=COMPLETE')+'）。PASS必须提供实际存在的证据路径；返修问题须给 problem_id、file、scenario、expected、close_condition。'
            +('执行角色必须在 test_commands 中列出本轮CLI工具事件实际成功执行的测试命令片段及 result="PASS"；不得把计划命令当作已执行。' if role=='EXEC' else '审计只读且不得运行测试，test_commands 与 evidence_outputs 必须为空数组。')+'\n'
            +('若本轮要求生成 evidence 文件，不要在 CLI 子沙箱中直接写证据文件；将每份 UTF-8 正文以 {"path":绝对路径,"content":正文} 放入 evidence_outputs。Coordinator 仅向本 run 预授权路径原子写入，正文需包含本轮 run_id、task_id、base_sha，单份不超过 64 KiB。' if role=='EXEC' else '')
            +'范围：'+task.get('scope','')+'\n'+(task.get('audit_scope','') if role in ('AUDIT','STAGE') else '')+'\n'+task.get('repair','')
            +('\n返修任务卡正文（Coordinator 已按批准目录读取并限制为 64 KiB）：\n'+task['repair_card_content'] if task.get('repair_card_content') else '')
            +('\n以下是 Coordinator 已按回执身份和 SHA 独立读取的上游只读证据。请在本独立上下文直接审查所附正文；引用路径只作为来源定位，不要要求 CLI 另行打开 Coordinator 私有状态目录：\n'+json.dumps(task.get('receipt_context',[]),ensure_ascii=False) if role in ('AUDIT','STAGE') else ''))

def acceptance_valid(home,state):
    path=home/'acceptance.json'
    if not path.is_file() or state.get('live_enabled') is not True or state.get('acceptance_sha256')!=digest(path): return False
    try:
        a=load(path); required={'continuity','repair','crash','blocked','permissions','stage','real_cli'}
        if a.get('status')!='PASS' or not required.issubset(set(a.get('passed',[]))): return False
        for file in ('coordinator.py','winjob.py','result.schema.json'):
            if a['code_sha256'][file]!=digest(home/file): return False
        for e in a['evidence']:
            p=pathlib.Path(e['path'])
            if not p.is_file() or not p.resolve().is_relative_to(home.resolve()) or digest(p)!=e['sha256']: return False
        return bool(a['evidence'])
    except (OSError,KeyError,ValueError,TypeError): return False

def launch(home,state,task,role,mode,codex,root):
    run_id=uuid.uuid4().hex; run=home/'runs'/run_id; run.mkdir(parents=True)
    base=git(root,'rev-parse','HEAD'); model, effort=MODELS[role]
    evidence_run_id=task.get('execution_run_id') if role=='AUDIT' and task.get('execution_run_id') else run_id
    expected=[str(x).replace('{run_id}',evidence_run_id) for x in task.get('expected_evidence',[])]; stage_evidence=[]; context_receipts=[]
    supplemental=task.get('supplemental_evidence',[])
    expected.extend(supplemental)
    evidence_before={}
    if role=='EXEC':
        collisions=[x for x in expected if pathlib.Path(x).exists()]
        if collisions:
            task.update(state='BLOCKED',block_reason='本轮证据目标已存在，为保护旧证据拒绝覆盖: '+collisions[0]); save(home,state); return
        evidence_before={x:None for x in expected}
    source_run_id=task.get('execution_run_id') if role=='AUDIT' else None
    source_receipt=None
    if source_run_id:
        source_path=home/'runs'/source_run_id/'verified.json'
        if not source_path.is_file(): raise ValueError('独立审计缺少执行阶段已核验回执')
        source_receipt=load(source_path)
        if source_receipt.get('task_id')!=task['task_id'] or source_receipt.get('role')!='EXEC' or source_receipt.get('run_id')!=source_run_id: raise ValueError('执行回执身份链不匹配')
        for item in source_receipt.get('evidence',[]):
            p=pathlib.Path(item['path'])
            if not p.is_file() or digest(p)!=item['sha256']: raise ValueError('独立审计前执行证据已变化')
        context_receipts.append(receipt_context(source_path))
        expected.append(str(source_path))
    if role=='AUDIT' and supplemental:
        allowed_supplement=home.resolve() if mode=='SIMULATED' else (VALIDATION_ROOT.resolve() if mode=='VALIDATION_CLI' else DELIVERY.parent.resolve())
        for item in supplemental:
            evidence_path=pathlib.Path(item).resolve()
            if not evidence_path.is_relative_to(allowed_supplement) and not evidence_path.is_relative_to(pathlib.Path(root).resolve()): raise ValueError('返修补充证据越过验证/交付/工作区根')
            if not evidence_path.is_file(): raise ValueError('返修补充证据缺失: '+str(evidence_path))
            if str(evidence_path) not in {x['path'] for x in context_receipts}: context_receipts.append(receipt_context(evidence_path))
    if role=='STAGE':
        members=[x for x in state['tasks'] if x['stage']==task['stage'] and x['kind'] not in ('STAGE_AUDIT','REPAIR')]
        if not members or any(x['state']!='PASS' or not x.get('run_id') for x in members): raise ValueError('阶段成员或审计回执缺失')
        for member in members:
            receipt=home/'runs'/member['run_id']/'verified.json'
            if not receipt.is_file(): raise ValueError('阶段成员缺少已核验审计回执')
            receipt_obj=load(receipt)
            if receipt_obj.get('task_id')!=member['task_id'] or receipt_obj.get('run_id')!=member['run_id'] or receipt_obj.get('role')!='AUDIT' or receipt_obj.get('verdict')!='PASS': raise ValueError('阶段成员审计回执身份或结论不匹配')
            if receipt_obj.get('source_run_id'):
                source=home/'runs'/receipt_obj['source_run_id']/'verified.json'
                if not source.is_file() or digest(source)!=receipt_obj.get('source_receipt_sha256'): raise ValueError('阶段成员执行与审计回执链校验失败')
                if str(source.resolve()) not in {x['path'] for x in context_receipts}: context_receipts.append(receipt_context(source))
            if str(receipt.resolve()) not in {x['path'] for x in context_receipts}: context_receipts.append(receipt_context(receipt))
            stage_evidence.append(str(receipt)); expected.append(str(receipt))
    prompt_task=dict(task)
    if role in ('AUDIT','STAGE'):
        if sum(len(x['content'].encode('utf-8')) for x in context_receipts)>262144: raise ValueError('上游回执总量超过独立审计上下文预算')
        prompt_task['receipt_context']=context_receipts
    if task.get('kind')=='REPAIR':
        card_path=pathlib.Path(task['card']).resolve()
        allowed_card_root=home.resolve() if mode=='SIMULATED' else (VALIDATION_ROOT.resolve() if mode=='VALIDATION_CLI' else DELIVERY.parent.resolve())
        if not card_path.is_relative_to(allowed_card_root): raise ValueError('自动返修卡不在验证/交付根')
        card_bytes=card_path.read_bytes()
        if len(card_bytes)>65536: raise ValueError('自动返修卡超过 64 KiB')
        prompt_task['repair_card_content']=card_bytes.decode('utf-8-sig')
    for key in ('scope','audit_scope','repair'):
        value=prompt_task.get(key,'')
        if isinstance(value,str): prompt_task[key]=value.replace('{run_id}',evidence_run_id).replace('{evidence0}',expected[0] if expected else '')
    prompt=prompt_for(prompt_task,role,base,root,run_id)
    if expected:
        prompt+='\n本任务要求输出以下真实证据文件，完成时 evidence_paths 必须包括：'+json.dumps(expected,ensure_ascii=False)
    if supplemental:
        prompt+='\n返修后新增证据如下，必须逐项复审并列入 evidence_paths：'+json.dumps(supplemental,ensure_ascii=False)
    if stage_evidence:
        prompt+='\n阶段审计必须逐项读取并检查下列每个成员的已核验审计回执、独立证据及通过范围；证据不足返回 BLOCKED，不可仅依据样本文件。输出的 evidence_paths 必须包含全部这些绝对路径：'+json.dumps(stage_evidence,ensure_ascii=False)
    cli_version='unknown'
    if mode!='SIMULATED':
        ver_env=os.environ.copy(); ver_env.update(TMP=TMP,TEMP=TMP,TMPDIR=TMP)
        try:
            version_result=subprocess.run([codex,'--version'],capture_output=True,text=True,encoding='utf-8',errors='replace',env=ver_env,timeout=15)
            if version_result.returncode==0: cli_version=version_result.stdout.strip()[:120]
        except (OSError,subprocess.TimeoutExpired): pass
    isolated_cli=(mode=='VALIDATION_CLI' and root.resolve().is_relative_to(VALIDATION_ROOT.resolve()) and home.resolve().is_relative_to(VALIDATION_ROOT.resolve()))
    spec={'run_id':run_id,'task_id':task['task_id'],'stage':task['stage'],'role':role,'mode':mode,'root':str(root),'cwd':str(root),'codex_home':os.environ.get('CODEX_HOME') or str(pathlib.Path.home()/'.codex'),'thread_source':'Codex CLI exec 默认来源（未指定/伪装）','codex_version':cli_version,'model':model,'effort':effort,'base_sha':base,'base_tree':git(root,'rev-parse','HEAD^{tree}'),'card_sha256':digest(pathlib.Path(task['card'])),'codex':codex,'prompt':prompt,'expected_evidence':expected,'evidence_before':evidence_before,'stage_evidence':stage_evidence,'source_run_id':source_run_id,'source_receipt_sha256':digest(home/'runs'/source_run_id/'verified.json') if source_run_id else None,'mock':task.get('mock',{}),'attempt':task.get('attempt',0),'started_at':now(),'started_at_ns':time.time_ns(),'live_execution_authorized':(isolated_cli or (mode=='REAL_CLI' and acceptance_valid(home,state))),'agents_sha256':digest(root/'AGENTS.md') if (root/'AGENTS.md').is_file() else None}
    atomic(run/'spec.json',spec)
    schema=load(home/'result.schema.json')
    for key,value in {'task_id':task['task_id'],'run_id':run_id,'scope_id':task['task_id'],'base_sha':base,'handoff':'WRITE_LEASE=RELEASED' if role=='EXEC' else 'AUDIT_HANDOFF=COMPLETE'}.items(): schema['properties'][key]['enum']=[value]
    atomic(run/'schema.json',schema)
    task.update(state='EXECUTING' if role=='EXEC' else 'AUDITING',run_id=run_id,role=role,model=model,effort=effort,base_sha=base,lease_id=run_id,updated_at=now()); save(home,state)
    print(f"[{now()}] 启动 {task['task_id']} | {role} | {model} | run={run_id}",flush=True)
    proc=subprocess.Popen([sys.executable,str(pathlib.Path(__file__).resolve()),'worker','--home',str(home),'--run-id',run_id],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,close_fds=True)
    last=None
    while proc.poll() is None:
        activity_path=run/'activity.json'
        if activity_path.is_file():
            activity=load(activity_path)
            signature=(activity.get('updated_at'),activity.get('event'))
            if signature!=last:
                last=signature
                suffix=(' | thread='+activity['thread_id']) if activity.get('thread_id') else ''
                print(f"[{activity.get('updated_at')}] {task['task_id']} | {activity.get('status','处理中')}{suffix}",flush=True)
        try: proc.wait(timeout=.5)
        except subprocess.TimeoutExpired: pass
    proc.wait()
    activity_path=run/'activity.json'
    if activity_path.is_file():
        activity=load(activity_path)
        if activity.get('thread_id'):
            print(f"[{now()}] 会话 thread_id={activity['thread_id']} | 可用 `codex resume {activity['thread_id']}` 按ID恢复；项目侧栏自动登记不受CLI保证",flush=True)
    consume(home,state,task,root)
    verdict=task.get('audit_verdict') or ('EXECUTED' if task.get('state')=='AWAITING_AUDIT' else task.get('state'))
    print(f"[{now()}] 任务终态 {task['task_id']} | {task.get('state')} | verdict={verdict}",flush=True)
    return

def consume(home,state,task,root):
    run=home/'runs'/task['run_id']; done=run/'exit.json'
    if not done.exists(): task.update(state='RECOVERY_REQUIRED',block_reason='子进程结束凭据缺失，写入结果不确定'); save(home,state); return
    result=load(done); spec=load(run/'spec.json'); role=spec['role']
    if result['exit_code']:
        n=task.get('retry_count',0)
        if result.get('safe_retry') and n<2:
            time.sleep(2**n)
            task.update(state='READY' if role=='EXEC' else 'AWAITING_AUDIT',retry_count=n+1,attempt=task.get('attempt',0)+1,block_reason='模拟的无副作用启动失败'); save(home,state); return
        reason=result.get('stop_reason','PROCESS_ERROR')
        if role!='EXEC' and result.get('tree_stopped') and reason in ('QUOTA','AUTH','PERMISSION'):
            task.update(state='BLOCKED',block_reason=reason+'：只读进程停止，等待限制解除')
        else: task.update(state='RECOVERY_REQUIRED',block_reason=reason+'：子进程异常退出，副作用未证明安全')
        save(home,state); return
    try:
        if result.get('tree_stopped') is not True: raise ValueError('未确认子进程树终止')
        if digest(pathlib.Path(task['card']))!=spec['card_sha256']: raise ValueError('任务卡在运行中发生变化')
        telemetry=load(run/'telemetry.json') if (run/'telemetry.json').is_file() else {}
        observed_commands=telemetry.get('commands',[])
        materialized=[]
        obj=validate_result(load(run/'last.json'),task,role,spec['base_sha'],spec['mode'],spec['expected_evidence'],run_id=spec['run_id'],observed_commands=observed_commands,materialize=lambda result:materialize_evidence_outputs(result,spec,role,materialized))
        head=git(root,'rev-parse','HEAD'); status=git(root,'status','--porcelain')
        if obj['result_sha']!=head: raise ValueError('结果 SHA 与实际 HEAD 不一致')
        if obj['tree_sha']!=git(root,'rev-parse','HEAD^{tree}'): raise ValueError('结果 tree 与实际 Git 不一致')
        if status: raise ValueError('工作树不干净，无法确认写入范围')
        if role!='EXEC' and head!=spec['base_sha']: raise ValueError('审计期间 HEAD 改变')
        if role=='EXEC' and head!=spec['base_sha']:
            if git(root,'merge-base','--is-ancestor',spec['base_sha'],head)!='': raise ValueError('基线不是祖先')
            paths=git(root,'diff','--name-only',spec['base_sha'],head).splitlines()
            allowed=task.get('allowed_paths',[])
            if not allowed or any(not any(p==a or p.startswith(a.rstrip('/')+'/') for a in allowed) for p in paths): raise ValueError('提交变更超出任务路径范围')
        for path in spec.get('expected_evidence',[]):
            p=pathlib.Path(path)
            if role=='EXEC' and obj['verdict']!='BLOCKED' and (spec.get('evidence_before',{}).get(path,'collision') is not None or not p.is_file() or p.stat().st_mtime_ns < spec['started_at_ns']):
                raise ValueError('本轮必需证据不是本run新生成，或生成时间早于本轮启动')
        if role=='AUDIT' and spec.get('source_run_id'):
            source_path=home/'runs'/spec['source_run_id']/'verified.json'
            if digest(source_path)!=spec.get('source_receipt_sha256'): raise ValueError('执行阶段回执在独立审计期间发生变化')
        obj['evidence_sha256']={str(pathlib.Path(p)):digest(pathlib.Path(p)) for p in obj['evidence_paths']}
        obj['evidence']=[{'path':str(pathlib.Path(p)),'sha256':digest(pathlib.Path(p)),'size_bytes':pathlib.Path(p).stat().st_size,'mtime_ns':pathlib.Path(p).stat().st_mtime_ns} for p in obj['evidence_paths']]
        obj['materialized_evidence']=materialized
        obj['verified_at']=now(); obj['role']=role; obj['source_run_id']=spec.get('source_run_id'); obj['source_receipt_sha256']=spec.get('source_receipt_sha256')
        obj['started_at']=spec.get('started_at'); obj['observed_commands']=observed_commands
        obj['execution_context']={'cli_version':spec.get('codex_version'),'codex_home':spec.get('codex_home'),'cwd':spec.get('cwd'),'thread_source':spec.get('thread_source'),'model':spec.get('model'),'effort':spec.get('effort'),'mode':spec.get('mode')}
        atomic(run/'verified.json',obj)
    except Exception as exc:
        task.update(state='RECOVERY_REQUIRED',block_reason='回执核验失败：'+str(exc)); save(home,state); return
    task.update(result_sha=head,lease_id=None,updated_at=now(),retry_count=0,audit_verdict=obj['verdict'] if role!='EXEC' else None,block_reason=None)
    if role=='EXEC': task['execution_run_id']=task['run_id']
    verdict=obj['verdict']
    if verdict=='BLOCKED': task.update(state='BLOCKED',block_reason=obj['next_action'])
    elif role=='EXEC': task['state']='AWAITING_AUDIT'
    elif verdict=='PASS':
        task['state']='PASS'
        if task.get('parent_id'):
            parent=next(t for t in state['tasks'] if t['task_id']==task['parent_id'])
            parent.update(state='AWAITING_AUDIT',block_reason=None,supplemental_evidence=obj['evidence_paths'])
        for target_id in task.get('resolves',[]):
            target=next(t for t in state['tasks'] if t['task_id']==target_id)
            if target['state']!='BLOCKED': raise RuntimeError('解除目标不是BLOCKED: '+target_id)
            target.update(state='AWAITING_AUDIT',block_reason=None,supplemental_evidence=obj['evidence_paths'])
    else:
        root_id=task.get('root_id',task['task_id']); origin=next(t for t in state['tasks'] if t['task_id']==root_id)
        counts=origin.setdefault('revision_counts',{}); exhausted=[]
        evidence=hashlib.sha256(''.join(sorted(obj['evidence_sha256'].values())).encode()).hexdigest()
        for finding in obj['findings']:
            issue=finding['problem_id']; prev=counts.get(issue,{})
            count=prev.get('count',0)+1 if prev.get('evidence')==evidence else 1
            counts[issue]={'count':count,'evidence':evidence}
            if count>2: exhausted.append(issue)
        total=origin.get('revision_total',0)+1; origin['revision_total']=total
        if exhausted or total>6:
            task.update(state='BLOCKED',block_reason='返修次数达到上限: '+','.join(exhausted)); origin.update(state='BLOCKED',block_reason=task['block_reason'])
        else:
            repair_id=task['task_id']+'-repair-'+str(total)+'-'+task['run_id'][:8]
            card=home/'返修任务'/(repair_id+'.md'); card.parent.mkdir(parents=True,exist_ok=True)
            text='''# 自动生成的定点返修任务\n\n'''+'原目标：'+task['task_id']+'\n审计运行：'+task['run_id']+'\n\n'
            for f in obj['findings']:
                text+='问题 '+f['problem_id']+'\n位置：'+f['file']+'\n场景：'+f['scenario']+'\n独立预期：'+f['expected']+'\n关闭条件：'+f['close_condition']+'\n\n'
            evidence_dir=home/'返修任务证据'/repair_id
            expected=[str(evidence_dir/'执行报告.md'),str(evidence_dir/'MANIFEST.json')]
            text+='原任务卡：'+task['card']+'\n原任务范围：'+task.get('scope','')+'\n上轮审计证据：'+json.dumps(obj['evidence_paths'],ensure_ascii=False)+'\n'
            text+='执行动作：'+obj['next_action']+'\n仅修复上述问题；不得扩大授权范围。保留原证据不覆盖；将新执行报告和清单分别写入：'+expected[0]+' 和 '+expected[1]+'。按原卡及A17封存、恢复并核验；最终JSON evidence_paths须包含两者。\n'
            card.write_text(text,encoding='utf-8')
            repair={'task_id':repair_id,'stage':task['stage'],'kind':'REPAIR','parent_id':task['task_id'],'root_id':root_id,'dependencies':[],'state':'READY','card':str(card),'scope':obj['next_action'],'allowed_paths':task.get('allowed_paths',[]),'actions':task.get('actions',[]),'expected_evidence':expected,'mock':task.get('mock',{}),'attempt':total,'retry_count':0}
            task.update(state='WAITING_REPAIR',attempt=total,block_reason=None)
            state['tasks'].append(repair)

    save(home,state)

def reconcile_queue(state, queue):
    tasks=queue.get('tasks')
    if not isinstance(tasks,list): raise ValueError('任务队列无效')
    entries={}
    for item in tasks:
        if not isinstance(item,dict) or not isinstance(item.get('task_id'),str) or item['task_id'] in entries: raise ValueError('任务ID无效或重复')
        if not isinstance(item.get('dependencies'),list) or not item.get('stage') or not item.get('kind') or not pathlib.Path(item.get('card','')).is_file(): raise ValueError('任务目录字段缺失')
        entries[item['task_id']]=item
    for item in tasks:
        if any(d not in entries or d==item['task_id'] for d in item['dependencies']): raise ValueError('任务依赖不存在或自引用')
        if any(d not in entries or d==item['task_id'] for d in item.get('resolves',[])): raise ValueError('解除目标不存在或自引用')
    seen=set(); active=set()
    def visit(i):
        if i in active: raise ValueError('任务依赖成环')
        if i in seen: return
        active.add(i)
        for d in entries[i]['dependencies']: visit(d)
        active.remove(i); seen.add(i)
    for i in entries: visit(i)
    current={t['task_id']:t for t in state['tasks'] if not t.get('parent_id')}
    if any(i not in entries for i in current): raise ValueError('目录删除了已有任务')
    immutable=('stage','kind','dependencies','card','scope','allowed_paths','actions','expected_evidence','resolves')
    for i,item in entries.items():
        if i in current:
            if any(current[i].get(k)!=item.get(k) for k in immutable): raise ValueError('任务目录已变化：'+i)
        else:
            if item.get('state') not in ('READY','AWAITING_AUDIT','STAGE_READY','BLOCKED'): raise ValueError('新任务初始状态不合法')
            state['tasks'].append(dict(item))
    for item in tasks:
        for dep in item['dependencies']:
            previous=entries[dep]
            if previous['stage']!=item['stage'] and previous['kind']!='STAGE_AUDIT':
                gates=[x['task_id'] for x in tasks if x['stage']==previous['stage'] and x['kind']=='STAGE_AUDIT']
                if len(gates)!=1 or gates[0] not in item['dependencies']: raise ValueError('跨阶段依赖缺阶段审计门：'+item['task_id'])

def validate_actions(task):
    allowed={'CODE_EDIT','DOC_EDIT','TEST_SYNTHETIC','GIT_COMMIT','GIT_PUSH_DEV'}
    actions=task.get('actions',[]); paths=task.get('allowed_paths',[])
    if not isinstance(actions,list) or not isinstance(paths,list) or not actions or not paths: return False
    if any(not isinstance(a,str) or a not in allowed for a in actions): return False
    for p in paths:
        if not isinstance(p,str) or '\\' in p: return False
        q=pathlib.PurePosixPath(p)
        if q.is_absolute() or '..' in q.parts or p in ('.','') or p.startswith('.git'): return False
    return True

def run_loop(home,mode,root,codex):
    with lock(home/'coordinator.lock'):
        state=load(home/'state.json'); queue=load(home/'task_queue.json')
        if state['mode']!=mode: raise RuntimeError('运行模式与状态不一致')
        reconcile_queue(state,queue); save(home,state)
        if mode=='REAL_CLI' and (root.resolve()!=ROOT.resolve() or home.resolve()!=DELIVERY.resolve()): raise RuntimeError('真实模式源码根或协调器目录不符')
        if mode=='VALIDATION_CLI' and not (root.resolve().is_relative_to(VALIDATION_ROOT.resolve()) and home.resolve().is_relative_to(VALIDATION_ROOT.resolve())): raise RuntimeError('真实烟测必须在隔离根内')
        if git(root,'branch','--show-current')!='dev': raise RuntimeError('必须在 dev 分支')
        if git(root,'status','--porcelain') and not any(t['state'] in ('EXECUTING','AUDITING') for t in state['tasks']):
            state['next_action']='工作树不干净，禁止启动'; save(home,state); return
        for task in state['tasks']:
            if task['state'] in ('EXECUTING','AUDITING'):
                # 未获得 writer 锁说明旧 worker 仍在运行；锁定任务，禁止重启。
                deadline=time.monotonic()+1800; noted=False
                while True:
                    try:
                        with lock(home/'writer.lock' if task['state']=='EXECUTING' else home/'runs'/task['run_id']/'audit.lock'): consume(home,state,task,root)
                        break
                    except RuntimeError:
                        if not noted:
                            task.update(block_reason='旧 worker 仍活动；等待其结束，不启动新写者'); save(home,state); noted=True
                        if time.monotonic()>=deadline: return
                        time.sleep(1)
        if git(root,'status','--porcelain'):
            state['next_action']='恢复后工作树不干净，禁止启动'; save(home,state); return
        while True:
            # 无法确认的进程可能仍持有写操作；任何未知副作用都冻结后续启动。
            if any(t['state']=='RECOVERY_REQUIRED' for t in state['tasks']): break
            byid={t['task_id']:t for t in state['tasks']}; eligible=[]
            for task in state['tasks']:
                if task['state']=='AWAITING_AUDIT' and all(byid.get(d,{}).get('state')=='PASS' for d in task['dependencies']):
                    eligible.append((task,'STAGE' if task['kind']=='STAGE_AUDIT' else 'AUDIT'))
            if not eligible:
                stages=sorted({t['stage'] for t in state['tasks'] if t['kind'] not in ('STAGE_AUDIT','REPAIR')})
                for stage in stages:
                    members=[t for t in state['tasks'] if t['stage']==stage and t['kind'] not in ('STAGE_AUDIT','REPAIR')]
                    stage_task=next((t for t in state['tasks'] if t['stage']==stage and t['kind']=='STAGE_AUDIT'),None)
                    if members and all(t['state']=='PASS' for t in members) and stage_task and stage_task['state']=='STAGE_READY' and all(byid.get(d,{}).get('state')=='PASS' for d in stage_task['dependencies']): eligible.append((stage_task,'STAGE')); break
            if not eligible:
                for task in state['tasks']:
                    if task['state']=='READY' and all(byid.get(d,{}).get('state')=='PASS' for d in task['dependencies']): eligible.append((task,'EXEC'))
            if not eligible: break
            task,role=eligible[0]
            if mode=='REAL_CLI' and role=='EXEC':
                if not validate_actions(task):
                    task.update(state='BLOCKED',block_reason='任务动作或写入路径未获准'); save(home,state); continue
            launch(home,state,task,role,mode,codex,root)
        state['next_action']='等待恢复或人工决策' if any(t['state'] in ('BLOCKED','RECOVERY_REQUIRED') for t in state['tasks']) else '队列无就绪任务'; save(home,state)
        print(f"[{now()}] 协调器停止 | {state['next_action']}",flush=True)

def main():
    p=argparse.ArgumentParser(description='Stock A 本地协调器；真实业务接管默认关闭')
    p.add_argument('command',choices=['run','status','worker']); p.add_argument('--home',type=pathlib.Path,default=DELIVERY); p.add_argument('--root',type=pathlib.Path,default=ROOT); p.add_argument('--mode',choices=['SIMULATED','VALIDATION_CLI','REAL_CLI'],default='REAL_CLI'); p.add_argument('--codex',default='codex'); p.add_argument('--run-id')
    a=p.parse_args()
    if a.command=='worker': worker(a.home,a.run_id)
    elif a.command=='status':
        state=load(a.home/'state.json')
        for task in state.get('tasks',[]):
            run_id=task.get('run_id')
            activity_path=a.home/'runs'/str(run_id)/'activity.json' if run_id else None
            if activity_path and activity_path.is_file():
                task['visible_session']=load(activity_path)
                thread_id=task['visible_session'].get('thread_id')
                if isinstance(thread_id,str) and thread_id:
                    task['visible_session']['resume_command']='codex resume '+thread_id
        print(json.dumps(state,ensure_ascii=False,indent=2))
    else: run_loop(a.home,a.mode,a.root,a.codex)
if __name__=='__main__': main()
