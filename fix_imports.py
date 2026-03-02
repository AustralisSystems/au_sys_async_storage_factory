from pathlib import Path


def fix_imports(directory):
    files = list(Path(directory).rglob("*.py"))
    print(f"Found {len(files)} files in {directory}")
    for path in files:
        content = path.read_text(encoding="utf-8")

        new_content = content
        # Replace from storage.shared with from storage.shared
        new_content = new_content.replace("from storage.shared", "from storage.shared")
        # Replace import storage.shared with import storage.shared
        new_content = new_content.replace("import storage.shared", "import storage.shared")

        if new_content != content:
            path.write_text(new_content, encoding="utf-8")
            print(f"Fixed imports in {path}")


if __name__ == "__main__":
    fix_imports("storage")
