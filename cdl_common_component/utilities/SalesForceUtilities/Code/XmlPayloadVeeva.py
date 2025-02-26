# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
login = """<?xml version="1.0" encoding="utf-8"?><env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
          <env:Body>
            <n1:login xmlns:n1="urn:partner.soap.sforce.com">
              <n1:username>$username</n1:username>
              <n1:password>$password</n1:password>
            </n1:login>
          </env:Body>
        </env:Envelope>"""

job = """<?xml version="1.0" encoding="UTF-8"?>
        <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
          <operation>$query_type</operation>
          <object>$tb_name</object>
          <concurrencyMode>Parallel</concurrencyMode>
          <contentType>$content_type</contentType>
        </jobInfo>
        """

jobclose = """<?xml version="1.0" encoding="UTF-8"?>
        <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
        <state>Closed</state>
        </jobInfo>"""