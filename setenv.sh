# Uso: source setenv.sh
# Configura las variables de entorno necesarias para PySpark en Windows

export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"
export PATH="$JAVA_HOME/bin:$PATH"
export HADOOP_HOME="/c/hadoop"
export PATH="$HADOOP_HOME/bin:$PATH"

# Activar el entorno virtual
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/venv/Scripts/activate"

export PYSPARK_PYTHON="$SCRIPT_DIR/venv/Scripts/python.exe"
export PYSPARK_DRIVER_PYTHON="$SCRIPT_DIR/venv/Scripts/python.exe"

echo "JAVA_HOME=$JAVA_HOME"
echo "HADOOP_HOME=$HADOOP_HOME"
echo "PYSPARK_PYTHON=$PYSPARK_PYTHON"
echo "Python: $(python --version)"
echo "Java: $(java -version 2>&1 | head -1)"
