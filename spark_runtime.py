import getpass
import os
import re
import shutil
import subprocess
from pathlib import Path


def _java_executable(java_home: str | os.PathLike[str]) -> Path:
    executable = "java.exe" if os.name == "nt" else "java"
    return Path(java_home) / "bin" / executable


def _prepend_path(path: Path) -> None:
    path_text = str(path)
    entries = os.environ.get("PATH", "").split(os.pathsep)
    if path_text not in entries:
        os.environ["PATH"] = path_text + os.pathsep + os.environ.get("PATH", "")


def configure_java_home() -> None:
    java_home = os.environ.get("JAVA_HOME")
    if java_home and _java_executable(java_home).exists():
        _prepend_path(Path(java_home) / "bin")
        return

    if shutil.which("java"):
        return

    try:
        import jdk4py
    except ImportError as exc:
        raise RuntimeError(
            "Java was not found. Install OpenJDK 17, set JAVA_HOME, or install "
            "jdk4py==17.0.9.2 on Windows."
        ) from exc

    java_home_path = Path(jdk4py.JAVA_HOME)
    os.environ["JAVA_HOME"] = str(java_home_path)
    _prepend_path(java_home_path / "bin")


def require_spark_compatible_java(max_major: int = 17) -> None:
    configure_java_home()
    proc = subprocess.run(
        ["java", "-version"],
        check=False,
        capture_output=True,
        text=True,
    )
    version_text = (proc.stderr or proc.stdout).splitlines()[0] if (proc.stderr or proc.stdout) else ""
    match = re.search(r'version "(\d+)', version_text)
    if not match:
        return

    major = int(match.group(1))
    if major > max_major:
        raise RuntimeError(
            f"Spark 3.5 requires a Java version no newer than {max_major}; found {version_text}. "
            "Install/select OpenJDK 17 and rerun the pipeline."
        )


def configure_hadoop_home(project_root: Path) -> str:
    if os.name != "nt":
        return ""

    hadoop_home = project_root / ".hadoop"
    hadoop_bin = hadoop_home / "bin"
    winutils = hadoop_bin / "winutils.exe"
    if not winutils.exists():
        return ""

    os.environ["HADOOP_HOME"] = str(hadoop_home)
    _prepend_path(hadoop_bin)
    hadoop_home_java = hadoop_home.as_posix()
    hadoop_bin_java = hadoop_bin.as_posix()
    return f"-Djava.library.path={hadoop_bin_java} -Dhadoop.home.dir={hadoop_home_java}"


def configure_spark_runtime(project_root: Path) -> str:
    require_spark_compatible_java()

    username = os.environ.get("HADOOP_USER_NAME") or os.environ.get("USER") or os.environ.get("USERNAME")
    username = username or getpass.getuser() or "spark"
    os.environ.setdefault("USER", username)
    os.environ.setdefault("HADOOP_USER_NAME", username)

    java_options = configure_hadoop_home(project_root)
    user_option = f"-Duser.name={username}"
    return f"{java_options} {user_option}".strip()
