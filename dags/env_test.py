import os

acess = os.getenv("minio_access_key")
secret =os.getenv("minio_secret_key")

print(f"Access Key: {acess}")
print(f"Secret Key: {secret}")