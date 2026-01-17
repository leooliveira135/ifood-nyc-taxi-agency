import zipfile
from pathlib import Path

def zip_folder(source_dir: str | Path, output_zip: str | Path) -> None:
    """
        Create a ZIP archive from a source directory. This method is used to package Python modules (e.g. `ifood/`) for AWS Glue --extra-py-files.

        Args:
            source_dir (str | Path): Directory to zip. Example: "src/ifood"
            output_zip (str | Path): Output ZIP file path. Example: "ifood_libs.zip"

        Raises:
            FileNotFoundError: If the source directory does not exist.
    """

    source_dir = Path(source_dir)
    output_zip = Path(output_zip)

    if not source_dir.is_dir():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file in source_dir.rglob("*"):
            if file.is_file():
                zipf.write(file, arcname=file.relative_to(source_dir.parent))

def unzip_file(zip_path: str | Path, extract_to: str | Path) -> None:
    """
        Extract the contents of a ZIP archive to a target directory.

        This method is intended for **local validation and debugging** of ZIP
        artifacts (for example, verifying Glue `--extra-py-files` packages).
        It should NOT be used at runtime inside AWS Glue jobs.

        Behavior:
        - Validates that the ZIP file exists
        - Ensures the destination directory exists
        - Preserves directory structure during extraction

        Typical use cases:
        - Validate `ifood_libs.zip` before uploading to S3
        - Inspect packaged Python modules locally
        - Debug `ModuleNotFoundError` issues in Glue

        Args:
            zip_path (str | Path): Path to the ZIP file to extract. Example: "ifood_libs.zip"
            extract_to (str | Path): Directory where the ZIP contents will be extracted. If the directory does not exist, it will be created. Example: "/tmp/ifood_debug"

        Raises:
            FileNotFoundError: If the ZIP file does not exist.
            zipfile.BadZipFile: If the file is not a valid ZIP archive.
    """
    zip_path = Path(zip_path)
    extract_to = Path(extract_to)

    if not zip_path.is_file():
        raise FileNotFoundError(f"ZIP file not found: {zip_path}")

    extract_to.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zipf:
        zipf.extractall(extract_to)
