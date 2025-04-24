# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Example of authenticating with SPN + Secret
Can be expanded to retrieve values from Key Vault or other sources
"""
import os
import time
from azure.identity import ClientSecretCredential

from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

client_id = os.environ.get('FABRIC_CLIENT_ID')
client_secret = os.environ.get('FABRIC_CLIENT_SECRET')
tenant_id = os.environ.get('FABRIC_TENANT_ID')
print(client_id)
print(client_secret)
print(tenant_id)
token_credential = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

# Sample values for FabricWorkspace parameters
workspace_id = os.environ.get('FABRIC_WORKSPACE_ID')
#environment = os.environ.get('TARGET_ENVIRONMENT_NAME')
repository_directory = ".\fabric-items"
item_type_in_scope = ["Lakehouse", "Notebook"]#, "Environment"]

# Paramos 1 minuto antes de cambiar los ids.
#time.sleep(60)

#aquí hay un error y es que no puedo saber que workspace id se ha generado en el momento de lanzar la actions (hay que ver como resolverlo).
#temporalmente se usa el workspace actions-001

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id=workspace_id,
#    environment=environment,
    repository_directory=repository_directory,
    item_type_in_scope=item_type_in_scope,
    token_credential=token_credential,
)

# Publish all items defined in item_type_in_scope
publish_all_items(target_workspace)

# Unpublish all items defined in item_type_in_scope not found in repository
unpublish_all_orphan_items(target_workspace)
