import re

class PatternValidator(object):

    def patter_validator(self,source,pattern):
        if "Q1" in source or "Q2" in source or "Q3" in source or "Q4" in source:
            s_date = pattern.split("Q")
            source_date = source.split("Q")
            if source.startswith(s_date[0]):
               ##For GZ file pattern
                ##pattern= re.compile(r"([1-4]{1})([2-9]\d{3})_([2-9]\d{3}((0[1-9]|1[012])(0[1-9]|1\d|2[0-8])|(0[13456789]|1[012])(29|30)|(0[13578]|1[02])31)|(([2-9]\d)(0[48]|[2468][048]|[13579][26])|(([2468][048]|[3579][26])00))0229)_([01]\d|2[0123])([0-5]\d){2}.txt.gz$")
                pattern = re.compile(r"([1-4]{1})([2-9]\d{3})_([2-9]\d{3}((0[1-9]|1[012])(0[1-9]|1\d|2[0-8])|(0[13456789]|1[012])(29|30)|(0[13578]|1[02])31)|(([2-9]\d)(0[48]|[2468][048]|[13579][26])|(([2468][048]|[3579][26])00))0229)_([01]\d|2[0123])([0-5]\d){2}.txt$")
                if pattern.search(source_date[1]):
                    return True

        elif source.startswith("impprod_owner.") and source.endswith("csv") and "YYYYMMDD_HHMMSS" in pattern:
           regex=pattern.replace(pattern,"([0-9]{4})(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])_(2[0-3]|[01][0-9])([012345][0-9])([012345][0-9]).([0-9]*).")
           pattern=re.compile(regex)
           if pattern.search(source):
               return True
           return False

        elif source.endswith("parquet") and "YYYYMMDDHHMMSSSSSSSS" in pattern:
           regex=pattern.replace(pattern,"((([a-zA-Z]*[_])[a-zA-Z]*)*)_([0-9]{4})(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])(2[0-3]|[01][0-9])([012345][0-9])([012345][0-9])([0-9]{6})_([0-9]*)")
           pattern=re.compile(regex)
           if pattern.search(source):
               return True
           return False

        elif source.endswith("parquet") and "YYYYMMDDHHMMSS" in pattern:
           regex=pattern.replace(pattern,"((([a-zA-Z]*[_])[a-zA-Z]*)*)_([0-9]{4})(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])(2[0-3]|[01][0-9])([012345][0-9])([012345][0-9])_([0-9]*)")
           pattern=re.compile(regex)
           if pattern.search(source):
               return True
           return False

        elif source.endswith("parquet") and "YYYYMMDD" in pattern:
           regex=pattern.replace(pattern,"([a-zA-Z_4]*)_([0-9]{4})(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])(-|_)(.*)")
           pattern=re.compile(regex)
           if pattern.search(source):
               return True
           return False

        elif "_C_" in source:
            s_date=pattern.split("C")
            source_date= source.split("C")
            if source_date[0]==s_date[0]:
                    return True

        elif source.startswith("FACT"):
            reg=pattern.replace("N", "[0-9]")
            pattern=re.compile(reg)
            if pattern.search(source):
                return True

        elif source.startswith("LRX.PROD") or source.startswith("NEW.PROD") or source.startswith("SAN.CUST") or source.startswith("GEOLK") or source.startswith("MPDLK") or source.startswith("PLNLK")or source.startswith("REJLK") or source.startswith("PRDLK"):
            if source.startswith("NEW.PROD"):
                start_string=pattern.split("RW")[0]
                replace_string= pattern[len(pattern.split("RW")[0]):]

            elif source.startswith("SAN.CUST"):
                start_string=pattern.split("W")[0]
                replace_string= pattern[len(pattern.split("W")[0]):]

            elif source.startswith("LRX.PROD"):
                start_string=pattern.split("W")[0]
                replace_string= pattern[len(pattern.split("W")[0]):]

            else:

                start_string=pattern.split(".")[0]
                replace_string= pattern[len(pattern.split(".")[0]):]
            reg=start_string+replace_string.replace("N", "[0-9]")
            search_pattern=re.compile(reg)
            if search_pattern.search(source):
                return True

        elif "MMYYYY" in pattern:
            regex= re.compile(r"(1[0-2]|0[1-9])([0-9]{4})")
            if regex.search(source):
                start_name=pattern.split("MMYYYY")[0]
                if source.startswith(start_name):
                    return True
            return False

        elif source.endswith("csv") and "YYYYMM" in pattern:
            regex = re.compile(r"([0-9]{4})(1[0-2]|0[1-9])")
            if regex.search(source):
                start_name=pattern.split("YYYYMM")[0]
                if source.startswith(start_name):
                    return True
            return False

        elif "N_YYYYMM" in pattern:
            regex=pattern.replace("N_YYYYMM","([0-9])_([0-9]{4})(1[0-2]|0[1-9])")
            pattern=re.compile(regex)
            if pattern.search(source):
                return True
            return False

        else:
            if source == pattern:
                return True

        return False

