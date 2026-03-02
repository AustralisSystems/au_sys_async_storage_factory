import os


def replace_in_file(file_path):
    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()
    except UnicodeDecodeError:
        return False

    new_content = content.replace("storage.shared", "storage.shared")

    if new_content != content:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(new_content)
        return True
    return False


def walk_and_replace(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".py") or file.endswith(".md") or file.endswith(".json"):
                file_path = os.path.join(root, file)
                if replace_in_file(file_path):
                    print(f"Updated: {file_path}")


if __name__ == "__main__":
    storage_dir = "C:\\github_development\\projects\\digital-angels-kit\\agentic_code_engine\\storage"
    walk_and_replace(storage_dir)
    print("Replacement complete.")
