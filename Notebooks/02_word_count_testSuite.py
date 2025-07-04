# Databricks notebook source
# MAGIC %run
# MAGIC ./01_word_count

# COMMAND ----------

# Databricks notebook source
class batchWCTestSuite():
    def __init__(self):
        self.base_data_dir = "dbfs:/FileStore/shared_uploads/himanshu.kumar1@tothenew.com"
        self.test_data_path = f"{self.base_data_dir}/data/text"
        self.dataset_path = f"{self.base_data_dir}/datasets"

    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        spark.sql("DROP TABLE IF EXISTS word_count_table")
        dbutils.fs.rm("/user/hive/warehouse/word_count_table", True)

        dbutils.fs.rm(self.test_data_path, True)
        dbutils.fs.mkdirs(self.test_data_path)
        print("Done\n")

    def ingestData(self, itr):
        print(f"\tStarting Ingestion for iteration {itr}...", end='')

        # Clean target folder
        dbutils.fs.rm(self.test_data_path, True)
        dbutils.fs.mkdirs(self.test_data_path)

        # Copy ONLY the selected file
        dbutils.fs.cp(f"{self.dataset_path}/text_data_{itr}.txt", 
                      self.test_data_path + "/", recurse=True)
        print("Done")

    def assertResult(self, expected_count):
        print(f"\tStarting validation...", end='')
        result = spark.sql("""
            SELECT SUM(count) as total 
            FROM word_count_table 
            WHERE SUBSTR(word, 1, 1) = 's'
        """).collect()[0][0]

        actual_count = result if result is not None else 0

        print(f"Expected: {expected_count}, Actual: {actual_count}")
        assert actual_count == expected_count, f"Test failed! Expected: {expected_count}, Found: {actual_count}"
        print("Passed")

    def runTests(self):
        self.cleanTests()
        wc = batchWC(base_data_dir=self.test_data_path)

        print("Testing first iteration of batch word count...") 
        self.ingestData(1)
        wc.wordCount()
        self.assertResult(25)  # Change this after verifying
        print("First iteration completed.\n")

        print("Testing second iteration of batch word count...") 
        self.ingestData(2)
        wc.wordCount()
        self.assertResult(32)  # Change this after verifying
        print("Second iteration completed.\n") 

        print("Testing third iteration of batch word count...") 
        self.ingestData(3)
        wc.wordCount()
        self.assertResult(37)  # Change this after verifying
        print("Third iteration completed.\n")


# ✅ Run test
bwcTS = batchWCTestSuite()
bwcTS.runTests()


# COMMAND ----------

# MAGIC %md
# MAGIC ### STREAMING TESTSUITE

# COMMAND ----------

# Databricks notebook source
class streamWCTestSuite():
    def __init__(self):
        self.base_data_dir = "dbfs:/FileStore/shared_uploads/himanshu.kumar1@tothenew.com"
        self.test_data_path = f"{self.base_data_dir}/data/text"
        self.dataset_path = f"{self.base_data_dir}/datasets"

    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        spark.sql("DROP TABLE IF EXISTS word_count_table")
        dbutils.fs.rm("/user/hive/warehouse/word_count_table", True)

        # Do NOT delete checkpoint — streaming depends on it
        dbutils.fs.rm(self.test_data_path, True)
        dbutils.fs.mkdirs(self.test_data_path)
        print("Done\n")

    def ingestData(self, itr):
        print(f"\tStarting Ingestion for iteration {itr}...", end='')

        # Do NOT delete previous files – keep appending!
        dbutils.fs.cp(
            f"{self.dataset_path}/text_data_{itr}.txt",
            self.test_data_path + "/",
            recurse=True
        )
        print("Done")


    def assertResult(self, expected_count):
        print(f"\tStarting validation...", end='')
        result = spark.sql("""
            SELECT SUM(count) as total 
            FROM word_count_table 
            WHERE SUBSTR(word, 1, 1) = 's'
        """).collect()[0][0]

        actual_count = result if result is not None else 0
        print(f"Expected: {expected_count}, Actual: {actual_count}")
        assert actual_count == expected_count, f"Test failed! Expected: {expected_count}, Found: {actual_count}"
        print("Passed")

    def runTests(self):
        import time
        self.cleanTests()
        wc = streamWC(base_data_dir=self.test_data_path)

        print("Testing first iteration of streaming word count...")
        self.ingestData(1)
        wc.wordCount().awaitTermination()  # Wait for trigger once
        self.assertResult(25)
        print("First iteration completed.\n")

        print("Testing second iteration of streaming word count...")
        self.ingestData(2)
        wc.wordCount().awaitTermination()
        self.assertResult(32)
        print("Second iteration completed.\n")

        print("Testing third iteration of streaming word count...")
        self.ingestData(3)
        wc.wordCount().awaitTermination()
        self.assertResult(37)
        print("Third iteration completed.\n")


swcTS = streamWCTestSuite()
swcTS.runTests()


# COMMAND ----------

