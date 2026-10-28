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
    with tmp.open('w', encoding='utf-8', newline='\n') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2); f.write('\n'); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)
def load(path):
    with path.open(encoding='utf-8-sig') as f: return json.load(f)
def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def clean(s):
    s = re.sub(r'(?i)(bearer\s+|api[_-]?key["\s:=]+|password["\s:=]+|token["\s:=]+)([^\s",}]+)', r'\1[REDACTED]', s)
    s = re.sub(r'(?i)(postgres(?:ql)?://[^:/\s]+:)[^@\s]+@', r'\1[REDACTED]@', s)
    return re.sub(r'\bsk-[A-Za-z0-9_-]{15,}\b','[REDACTED]',s)
def write_clean(path, data): path.write_text(clean(data), encoding='utf-8')
def git(root, *args):
    p = subprocess.run(['git', '-c', 'core.quotePath=false', '-C', str(root), *args], capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=30)
    if p.returncode: raise RuntimeError('Git 查询失败: ' + clean(p.stderr))
    return p.stdout.strip()
@contextmanager
def lock(path):
    import msvcrt
    path.parent.mkdir(parents=True, exist_ok=True)
    f = path.open('a+b'); f.seek(0); f.write(b'0'); f.flush(); f.seek(0)
    try:
        try: msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError: raise RuntimeError('已有活动协调器或写者，拒绝重入')
        yield
    finally:
        try: f.seek(0); msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError: pass
        f.close()

def save(home, state): state['updated_at']=now(); atomic(home/'state.json', state)
def validate_result(obj, task, role, base, mode, required_evidence):
    if not isinstance(obj,dict) or obj.get('task_id')!=task['task_id'] or obj.get('base_sha')!=base: raise ValueError('结果任务或基线不匹配')
    if obj.get('verdict') not in ('PASS','NEEDS_REVISION','BLOCKED'): raise ValueError('结论不合法')
    if not isinstance(obj.get('findings'),list) or not isinstance(obj.get('evidence_paths'),list) or not isinstance(obj.get('next_action'),str): raise ValueError('结果字段缺失')
    if not isinstance(obj.get('result_sha'),str) or not isinstance(obj.get('tree_sha'),str): raise ValueError('结果 SHA 或 tree 缺失')
    if obj.get('handoff') != ('WRITE_LEASE=RELEASED' if role=='EXEC' else 'AUDIT_HANDOFF=COMPLETE'): raise ValueError('交接标记缺失')
    if obj.get('scope_id')!=task['task_id']: raise ValueError('审计或执行范围不匹配')
    if role=='EXEC' and obj['verdict']=='NEEDS_REVISION': raise ValueError('执行不能产生审计返修结论')
    if obj['verdict']=='PASS' and not obj['evidence_paths']: raise ValueError('PASS 缺少证据')
    if (mode in ('REAL_CLI','VALIDATION_CLI') or role=='STAGE') and obj['verdict']=='PASS':
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
        if kind=='thread.started': return kind,'会话已创建',event.get('thread_id')
        if kind=='turn.started': return kind,'模型开始处理',None
        if kind=='item.started': return kind,('开始 '+item_type if item_type else '开始处理步骤'),None
        if kind=='item.completed':
            if item_type=='agent_message': return kind,'模型已生成阶段回复',None
            if item_type=='error': return kind,'子任务返回错误',None
            return kind,('完成 '+item_type if item_type else '处理步骤完成'),None
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
        log_lock=__import__('threading').Lock()
        def on_output(stream,line):
            with log_lock:
                with (run/('stdout.log' if stream=='stdout' else 'stderr.log')).open('a',encoding='utf-8',newline='') as f:
                    f.write(clean(line)); f.flush()
                if stream=='stdout':
                    summary=public_event(line)
                    if summary:
                        kind,label,thread_id=summary
                        activity.update(event=kind,status=label,updated_at=now())
                        if thread_id: activity['thread_id']=thread_id
                        atomic(run/'activity.json',activity)
                elif line.strip():
                    activity.update(event='stderr',status='CLI有诊断输出（详情见受控运行日志）',updated_at=now())
                    atomic(run/'activity.json',activity)
        try:
            if mode=='SIMULATED':
                proc=winjob.run([sys.executable, str(home/'fake_cli.py'), str(run/'spec.json')],env=env,timeout=60,cwd=spec['root'],on_output=on_output)
            else:
                if role=='EXEC' and not spec.get('live_execution_authorized'): raise RuntimeError('真实业务写入尚未完成接管验收')
                args=[spec['codex'],'exec','--sandbox', 'workspace-write' if role=='EXEC' else 'read-only','--cd',spec['root'], '--model',spec['model'],'--config','model_reasoning_effort="'+spec['effort']+'"','--config','project_doc_max_bytes=131072','--json','--output-schema',str(run/'schema.json'),'-']
                if role=='EXEC': args[2:2]=['--add-dir', TMP, '--add-dir', str(home)]
                prompt=spec['prompt']
                proc=winjob.run(args,input_text=prompt,env=env,timeout=1800,cwd=spec['root'],on_output=on_output)
            if proc.returncode==0:
                if mode=='SIMULATED': candidate=proc.stdout
                else:
                    messages=[]
                    for line in proc.stdout.splitlines():
                        try:
                            event=json.loads(line); item=event.get('item',{})
                            if event.get('type')=='item.completed' and item.get('type')=='agent_message': messages.append(item.get('text',''))
                        except (ValueError,AttributeError): continue
                    if not messages: raise ValueError('CLI 未返回结构化最终结果')
                    candidate=messages[-1]
                if clean(candidate)!=candidate: raise ValueError('最终结果可能含凭据，拒绝归档原文')
                atomic(run/'last.json',json.loads(candidate))
            reason,safe_retry=classify_failure(proc.stdout,proc.stderr,pathlib.Path(spec['root']),spec['base_sha']) if proc.returncode and mode!='SIMULATED' else (('NONE',False) if proc.returncode==0 else ('PROCESS_ERROR',mode=='SIMULATED' and proc.returncode==75))
            if mode=='SIMULATED' and proc.returncode and 'quota' in proc.stderr.lower(): reason='QUOTA'
            atomic(run/'exit.json',dict(marker,exit_code=proc.returncode,finished_at=now(),safe_retry=safe_retry,tree_stopped=True,stop_reason=reason))
        except Exception as exc:
            write_clean(run/'stderr.log',type(exc).__name__+': '+str(exc)); atomic(run/'exit.json',dict(marker,exit_code=-1,finished_at=now(),safe_retry=isinstance(exc,FileNotFoundError),tree_stopped=False))

def prompt_for(task, role, base, root):
    return ('你是 Stock A 有界'+role+'上下文。先完整读取 '+str(root/'AGENTS.md')+'、'+str(root/'PROJECT_SPEC.md')+' 和以下任务卡：'+task['card']+'。'
            '不得创建后继，不得修改旧V4、来源、生产PG、正式Head或部署，不得绕过审批。审计只读，不执行测试、诊断或构建；只输出JSON。'
            'Windows工具可能从其他当前目录启动；所有文件使用绝对路径，所有Git命令显式使用 git -C "'+str(root)+'"，不得假设当前目录。先实际用 git -C 核对 HEAD 和 HEAD^{tree}。'
            '任务ID '+task['task_id']+'，基线 '+base+'。固定输出 task_id,base_sha,result_sha,verdict,findings,evidence_paths,next_action。'
            '字段另含 tree_sha、scope_id（填任务ID）、handoff（本角色必须填 '+('WRITE_LEASE=RELEASED' if role=='EXEC' else 'AUDIT_HANDOFF=COMPLETE')+'）。PASS必须提供实际存在的证据路径；返修问题须给 problem_id、file、scenario、expected、close_condition。\n'
            '范围：'+task.get('scope','')+'\n'+task.get('repair',''))

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
    expected=list(task.get('expected_evidence',[])); stage_evidence=[]
    supplemental=task.get('supplemental_evidence',[])
    expected.extend(supplemental)
    if role=='STAGE':
        members=[x for x in state['tasks'] if x['stage']==task['stage'] and x['kind'] not in ('STAGE_AUDIT','REPAIR')]
        if not members or any(x['state']!='PASS' or not x.get('run_id') for x in members): raise ValueError('阶段成员或审计回执缺失')
        for member in members:
            receipt=home/'runs'/member['run_id']/'verified.json'
            if not receipt.is_file(): raise ValueError('阶段成员缺少已核验审计回执')
            stage_evidence.append(str(receipt)); expected.append(str(receipt))
    prompt=prompt_for(task,role,base,root)
    if expected:
        prompt+='\n本任务要求输出以下真实证据文件，完成时 evidence_paths 必须包括：'+json.dumps(expected,ensure_ascii=False)
    if supplemental:
        prompt+='\n返修后新增证据如下，必须逐项复审并列入 evidence_paths：'+json.dumps(supplemental,ensure_ascii=False)
    if stage_evidence:
        prompt+='\n阶段审计必须逐项读取并检查下列每个成员的已核验审计回执、独立证据及通过范围；证据不足返回 BLOCKED，不可仅依据样本文件。输出的 evidence_paths 必须包含全部这些绝对路径：'+json.dumps(stage_evidence,ensure_ascii=False)
    spec={'run_id':run_id,'task_id':task['task_id'],'stage':task['stage'],'role':role,'mode':mode,'root':str(root),'model':model,'effort':effort,'base_sha':base,'base_tree':git(root,'rev-parse','HEAD^{tree}'),'card_sha256':digest(pathlib.Path(task['card'])),'codex':codex,'prompt':prompt,'expected_evidence':expected,'stage_evidence':stage_evidence,'mock':task.get('mock',{}),'attempt':task.get('attempt',0),'live_execution_authorized':((mode=='VALIDATION_CLI' and task.get('kind')=='SMOKE') or (mode=='REAL_CLI' and acceptance_valid(home,state))),'agents_sha256':digest(root/'AGENTS.md') if (root/'AGENTS.md').is_file() else None}
    atomic(run/'spec.json',spec)
    schema=load(home/'result.schema.json')
    for key,value in {'task_id':task['task_id'],'scope_id':task['task_id'],'base_sha':base,'handoff':'WRITE_LEASE=RELEASED' if role=='EXEC' else 'AUDIT_HANDOFF=COMPLETE'}.items(): schema['properties'][key]['enum']=[value]
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
            print(f"[{now()}] 会话可在Codex中按ID打开: {activity['thread_id']}",flush=True)
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
        obj=validate_result(load(run/'last.json'),task,role,spec['base_sha'],spec['mode'],spec['expected_evidence'])
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
        obj['evidence_sha256']={str(pathlib.Path(p)):digest(pathlib.Path(p)) for p in obj['evidence_paths']}
        atomic(run/'verified.json',obj)
    except Exception as exc:
        task.update(state='RECOVERY_REQUIRED',block_reason='回执核验失败：'+str(exc)); save(home,state); return
    task.update(result_sha=head,lease_id=None,updated_at=now(),retry_count=0,audit_verdict=obj['verdict'] if role!='EXEC' else None,block_reason=None)
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
            if activity_path and activity_path.is_file(): task['visible_session']=load(activity_path)
        print(json.dumps(state,ensure_ascii=False,indent=2))
    else: run_loop(a.home,a.mode,a.root,a.codex)
if __name__=='__main__': main()
