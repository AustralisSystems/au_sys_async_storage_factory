import os
import glob

directory = r"c:\github_development\projects\AustralisSystems\au_sys_async_storage_factory"
for path in glob.glob(directory + "/**/*.py", recursive=True):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    if "storage" in content:
        content = content.replace("storage", "storage")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

print("Replacement complete.")
