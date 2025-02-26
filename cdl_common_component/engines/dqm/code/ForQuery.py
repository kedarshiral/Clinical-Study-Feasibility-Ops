#sparksession variable is available as "spark"
import sys
import CommonConstants as CommonConstants


def dqm(spark, val_path, threshold, qc_query, threshold_type,status_message):
    validation_error_path = val_path
    status = CommonConstants.STATUS_FAILED
    record_count = 0
    error_record_df = spark.sql(qc_query)

    try:
        if threshold is not None or threshold != "":
            record_count = error_record_df.count()
        else:
            status_message = "There is no threshold configured for this check - hence DQM check is invalid"
            logger.debug(status_message, extra=self.execution_context.get_context())

        if threshold_type.lower() == "count":
            if record_count > threshold:
                error_record_df.write.mode('overwrite').format("parquet").option("header", "true").save(validation_error_path)
                status = CommonConstants.STATUS_FAILED
            elif record_count >= 0 and record_count <= threshold:
                status = CommonConstants.STATUS_SUCCEEDED
        elif threshold_type.lower() == "percent":
            record_count=0
            temp_df = error_record_df.select("variance")
            max_temp_val = temp_df.agg({"variance":"max"}).collect()[0].asDict()['max(variance)']
            if max_temp_val > threshold:
                status = CommonConstants.STATUS_FAILED
                error_record_df = error_record_df.filter("variance>" + str(threshold))
                record_count = error_record_df.count()
                error_record_df.write.mode('overwrite').format("parquet").option("header", "true").save(validation_error_path)
            else:
                status = CommonConstants.STATUS_SUCCEEDED

        else:
            status_message = "Threshold type configured is invalid"
            logger.debug(status_message, extra=self.execution_context.get_context())

        summary_statement= str(record_count) + " records " + status + " " + str(status_message)

        result = {
                "validation_message": summary_statement,
                "error_record_count": record_count,
                "validation_status" : status,
                "error_details_path": validation_error_path
                }

        return result
    except Exception as ex:
        print(str(ex))
        raise ex

