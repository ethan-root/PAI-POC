import os
import sys
import json
import subprocess

def run_aliyun_command(args):
    """Run aliyun CLI command and return the JSON result."""
    try:
        # Construct the full command
        cmd = ["aliyun"] + args
        print(f"Executing: {' '.join(cmd)}")
        
        # Execute without shell=True to avoid escaping issues
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Command failed with exit code {result.returncode}")
            print("STDERR:", result.stderr)
            print("STDOUT:", result.stdout)
            return None
            
        return result.stdout
    except Exception as e:
        print(f"Exception during command execution: {e}")
        return None

def main():
    # Load configuration from environment variables
    job_name = os.environ.get("JOB_NAME")
    entry_class = os.environ.get("ENTRY_CLASS")
    jar_oss_path = os.environ.get("jar_oss_path")
    flink_namespace = os.environ.get("FLINK_NAMESPACE")
    flink_workspace = os.environ.get("FLINK_WORKSPACE")
    flink_endpoint = os.environ.get("FLINK_ENDPOINT")

    if not all([job_name, entry_class, jar_oss_path, flink_namespace, flink_workspace, flink_endpoint]):
        print("Error: Missing required environment variables.")
        sys.exit(1)

    print(f"Deploying Job: {job_name}")
    print(f"Namespace: {flink_namespace}")

    # Construct Deployment JSON
    # Construct Deployment JSON
    # Based on the GET response, the API expects a flat structure, not K8s-style metadata/spec
    deployment_json = {
        "name": job_name,
        "namespace": flink_namespace,
        "artifact": {
            "kind": "JAR",
            "jarUri": jar_oss_path,
            "entryClass": entry_class
        },
        "deploymentTarget": {
            "mode": "PER_JOB",
            "name": "default-queue"
        },
        "flinkConf": {
            "parallelism.default": "1",
            "jobmanager.memory.process.size": "1g",
            "taskmanager.memory.process.size": "1g" 
        }
    }

    json_body = json.dumps(deployment_json)
    print("Deployment Config:")
    print(json_body)

    # 1. Check existing deployment
    check_url = f"/api/v2/namespaces/{flink_namespace}/deployments"
    check_args = [
        "ververica", "GET", check_url,
        "--endpoint", flink_endpoint,
        "--header", f"workspace={flink_workspace}"
    ]
    
    list_output = run_aliyun_command(check_args)
    deployment_id = None
    
    if list_output:
        try:
            print(f"GET Response length: {len(list_output)}")
            try:
                data = json.loads(list_output)
                print(f"Parsed response type: {type(data)}")
                
                deployments = []
                if isinstance(data, list):
                    deployments = data
                elif isinstance(data, dict):
                    inner_data = data.get("data")
                    if isinstance(inner_data, dict):
                        deployments = inner_data.get("deployments", [])
                    elif isinstance(inner_data, list):
                         deployments = inner_data
                    elif "deployments" in data:
                        deployments = data.get("deployments", [])
                
                print(f"Found {len(deployments)} deployments.")
                
                if len(deployments) > 0:
                    print("DEBUG: Full JSON of first existing deployment:")
                    print(json.dumps(deployments[0], indent=2))
                
                for dep in deployments:
                    if not isinstance(dep, dict):
                        continue
                    metadata = dep.get("metadata", {})
                    if metadata.get("name") == job_name:
                        deployment_id = metadata.get("id")
                        break
            except Exception as e:
                print(f"Error parsing deployment list: {e}")
                print(f"Raw output snippet: {list_output[:500]}")
        except json.JSONDecodeError:
            print("Failed to parse GET response")

    # 2. Update or Create
    if deployment_id:
        print(f"Found existing deployment ID: {deployment_id}. Updating...")
        update_url = f"/api/v2/namespaces/{flink_namespace}/deployments/{deployment_id}"
        deploy_args = [
            "ververica", "PATCH", update_url,
            "--endpoint", flink_endpoint,
            "--header", f"workspace={flink_workspace}",
            "--header", "Content-Type=application/json",
            "--body", json_body
        ]
    else:
        print("No existing deployment found. Creating new...")
        create_url = f"/api/v2/namespaces/{flink_namespace}/deployments"
        deploy_args = [
            "ververica", "POST", create_url,
            "--endpoint", flink_endpoint,
            "--header", f"workspace={flink_workspace}",
            "--header", "Content-Type=application/json",
            "--body", json_body
        ]

    result_output = run_aliyun_command(deploy_args)
    if not result_output:
        print("Deployment command failed.")
        sys.exit(1)

    print("Response:")
    print(result_output)

    # 3. Verify Success
    try:
        result_json = json.loads(result_output)
        if result_json.get("success") is not True:
            print("API returned success=false")
            print(f"Error Code: {result_json.get('errorCode')}")
            print(f"Error Message: {result_json.get('errorMessage')}")
            sys.exit(1)
        else:
            print("Deployment Successful!")
    except json.JSONDecodeError:
        print("Failed to parse deployment response")
        sys.exit(1)

if __name__ == "__main__":
    main()
