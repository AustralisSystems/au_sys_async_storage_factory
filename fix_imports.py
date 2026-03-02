import os
import glob


def fix_imports(directory):
    files = glob.glob(directory + "/**/*.py", recursive=True)
    print(f"Found {len(files)} files in {directory}")
    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        new_content = content
        # Replace from storage.shared with from storage.shared
        new_content = new_content.replace("from storage.shared", "from storage.shared")
        # Replace import storage.shared with import storage.shared
        new_content = new_content.replace("import storage.shared", "import storage.shared")

        if new_content != content:
            with open(path, "w", encoding="utf-8") as f:
                f.write(new_content)
            print(f"Fixed imports in {path}")


if __name__ == "__main__":
    fix_imports("storage")
