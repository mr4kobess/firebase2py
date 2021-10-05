import itertools
import multiprocessing
import os
import argparse
import subprocess
from pathlib import Path
from rich.console import Console
from rich.progress import BarColumn, Progress, RenderableColumn, SpinnerColumn, TimeElapsedColumn, TimeRemainingColumn
import sys
from time import sleep
from datetime import datetime, time
from firebase_dump import SubDom

console = Console()

os.environ['PYTHONIOENCODING'] = 'utf-8'
os.environ['VISIBLE_PROGRESS'] = '1'

PATH_DUMPS = 'data/'

colors_status = {'ok': '[green]',
                 'fail': '[red]',
                 'notfound': '[yellow]'}


def get_count_rows(file):
    c = 0
    line = file.readline()
    while line:
        if line.strip(' \t\n'):
            c += 1
        line = file.readline()
    return c


def read_lines_to_json(file):
    for line in file:
        if line.strip(' \t\n'):
            try:
                yield line.strip('\n\t ')
            except Exception as e:
                console.print_exception(show_locals=True)
                continue


def get_tables_from_server(server):
    _res = []
    for table in server['tables']:
        _res.append((server['IP'], table))
    return _res


def get_name_from_status(server_ip, name, status, dump_length=None):
    if status == 'ok':
        path_name = os.path.join(
            os.getcwd(), PATH_DUMPS, f"{server_ip}_{name}_ok.json")
    elif status == 'fail':
        path_name = os.path.join(
            os.getcwd(), PATH_DUMPS, f"{server_ip}_{name}_fail_{dump_length}.json")
    elif status == 'notfound':
        path_name = os.path.join(
            os.getcwd(), PATH_DUMPS, f"{server_ip}_{name}_fail_notfound.json")
    else:
        path_name = os.path.join(
            os.getcwd(), PATH_DUMPS, f"{server_ip}_{name}.json")
    return path_name


def generate_cmd(ip):
    cmd = f'python3 memcashed_dump.py --path {os.path.join(PATH_DUMPS, ip, f"{ip}.json")} --host {ip}'
    return cmd


def action_for_dump(server_ip, table):
    path_dump = Path(get_name_from_status(
        server_ip, table['table_name'], status='check'))
    if not path_dump.exists():
        Path(get_name_from_status(
            server_ip, table['table_name'], status='notfound')).touch()
        return server_ip, table['table_name'], '-', 'notfound'
    dump_length = get_count_rows(str(path_dump))
    if dump_length >= table['count']:
        path_dump.rename(
            get_name_from_status(server_ip, table['table_name'], dump_length=dump_length, status='ok'))
        return server_ip, table['table_name'], str(dump_length), 'ok'
    else:
        path_dump.rename(
            get_name_from_status(server_ip, table['table_name'], dump_length=dump_length, status='fail'))
        return server_ip, table['table_name'], str(dump_length), 'fail'


def run_cmd_dump(cmd):
    subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)
    return cmd


def pool_next(it):
    while True:
        answer = 'ERROR'
        try:
            answer = it.next(timeout=999999)
        except StopIteration:
            break
        yield answer

# @logger.catch


def run(data):
    sub_domain, verbose = data[0], data[1]
    if verbose > 1:
        console.print(f"[green]START - {sub_domain}")
    try:
        sd = SubDom(sub_domain)
        if sd.is_valid:
            os.makedirs(f'data/{sub_domain}', exist_ok=True)
            sd.dump()
    except Exception as e:
        return f"ERROR - {e} - " + sub_domain

    return 'DONE - ' + sub_domain


def main(args):
    path_to_firebase_file = args.path
    path_dumps = os.path.join(PATH_DUMPS)

    if not os.path.exists(path_dumps):
        os.mkdir(path_dumps)

    total_lines = get_count_rows(
        open(path_to_firebase_file, encoding='utf-8'))
    file = open(path_to_firebase_file, encoding='utf-8')

    progress = Progress(
        SpinnerColumn(spinner_name='earth'),
        "[progress.description]{task.description}",
        BarColumn(),
        "[progress.completed][cyan]{task.completed}/{task.total}[/] строк |",
        TimeElapsedColumn(),
        TimeRemainingColumn()
    )
    task_id = progress.add_task(description=f'Working...', total=total_lines)
    pool = multiprocessing.Pool(processes=args.cores)
    try:
        results = pool.imap(run, [(ip, args.verbose) for ip in read_lines_to_json(file)])
        done = 0
        with progress:
            for answer in pool_next(results):
                if args.verbose > 0:
                    clr = 'green' if 'DONE' in answer else 'red'
                    progress.console.print("\n" + answer, style=clr)
                if 'DONE' in answer:
                    done += 1
                    progress.tasks[task_id].description = f"({done}) Working.."
                progress.advance(task_id)

    except Exception as e:
        pool.terminate()
        console.print_exception()
        sys.exit(1)
    pool.close()
    pool.join()
    pool.terminate()

    console.print('Done!', style='green')


if __name__ == '__main__':
    multiprocessing.freeze_support()
    multiprocessing.set_start_method('spawn')

    parser = argparse.ArgumentParser(description="php2py")
    parser.add_argument("--path", dest="path", required=True)
    parser.add_argument("--cores", dest="cores", default=8, type=int)
    parser.add_argument("--verbose", dest="verbose",
                        default=1, type=int)
    args = parser.parse_args()
    console.rule(f"Info Generated {datetime.now().ctime()}")
    try:
        main(args)
    except KeyboardInterrupt:
        sys.exit()
