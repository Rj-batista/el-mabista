# Databricks notebook source
if not any(mount.mountPoint == '/mnt/groupe3' for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
            source = "wasbs://groupe3@esgidatas.blob.core.windows.net",
            mount_point = "/mnt/groupe3",
            extra_configs = {"fs.azure.account.key.esgidatas.blob.core.windows.net":dbutils.secrets.get(scope = "ScopeESGI", key = "elmata")})
