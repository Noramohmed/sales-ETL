#!/bin/bash
# download_jars.sh
# ─────────────────────────────────────────────────────────────────
# يحمّل الـ JARs المطلوبة للـ Snowflake Spark Connector
# شغّله مرة واحدة قبل docker-compose up
# ─────────────────────────────────────────────────────────────────

set -e

JARS_DIR="$(dirname "$0")/jars"
mkdir -p "$JARS_DIR"

echo "📦 Downloading Snowflake Spark Connector JARs ..."

# Snowflake Spark Connector (for Spark 3.2 / Scala 2.12)
SPARK_SF_JAR="spark-snowflake_2.12-2.14.0-spark_3.2.jar"
SPARK_SF_URL="https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.14.0-spark_3.2/${SPARK_SF_JAR}"

# Snowflake JDBC Driver
JDBC_JAR="snowflake-jdbc-3.15.0.jar"
JDBC_URL="https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.15.0/${JDBC_JAR}"

if [ ! -f "$JARS_DIR/$SPARK_SF_JAR" ]; then
    echo "⬇️  Downloading $SPARK_SF_JAR ..."
    curl -L "$SPARK_SF_URL" -o "$JARS_DIR/$SPARK_SF_JAR"
    echo "✅ $SPARK_SF_JAR downloaded."
else
    echo "✅ $SPARK_SF_JAR already exists."
fi

if [ ! -f "$JARS_DIR/$JDBC_JAR" ]; then
    echo "⬇️  Downloading $JDBC_JAR ..."
    curl -L "$JDBC_URL" -o "$JARS_DIR/$JDBC_JAR"
    echo "✅ $JDBC_JAR downloaded."
else
    echo "✅ $JDBC_JAR already exists."
fi

echo ""
echo "🎉 All JARs ready in: $JARS_DIR"
ls -lh "$JARS_DIR"
