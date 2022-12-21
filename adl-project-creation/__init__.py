import logging
import azure.functions as func
import json
from .project_creation import project_creation
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        req_body = req.get_json()
    except Exception as e:
        logging.exception(e)
        return func.HttpResponse('Unsupported request', status_code=400)
    project_name = req_body.get('project_name')
    project_type = req_body.get('project_type')
    storage_account_name = req_body.get('storage_account_name')
    storage_account_key = req_body.get('storage_account_key')
    missing = set()
    if project_name is None:
        missing.add('project_name')
    if project_type is None:
        missing.add('project_type')
    if storage_account_name is None:
        missing.add('storage_account_name')
    if storage_account_key is None:
        missing.add('storage_account_key')
    if len(missing) > 0:
        return func.HttpResponse(f"Missing {', '.join(missing)}", status_code=400)    
    try:
        pc = project_creation(storage_account_name,storage_account_key)
        project_adl_path = pc.project_intialization(project_name,project_type)
        res = {
            'msg': "Project creation successful",
            'project_adl_path': project_adl_path
        }
        return func.HttpResponse(json.dumps(res))
    except Exception as e:
        # logging.exception(e)
        print("init error")
        return func.HttpResponse(str(e), status_code=500)
