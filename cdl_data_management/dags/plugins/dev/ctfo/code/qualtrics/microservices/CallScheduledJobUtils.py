import sys
from SurveyScheduledJobUtils import SurveyScheduledJobUtils


def qual_scheduler(sys_user, **kwargs):
    SurveyScheduledJobUtils(sys_user).start_schedule_job()
