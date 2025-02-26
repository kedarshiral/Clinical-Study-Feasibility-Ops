$(document).ready(function (){
    var loc = window.location.pathname;
    var index = loc.lastIndexOf("/");
    var path = loc.substr(index);
   $('#configuration_table').dataTable( {
     "ajax": {
         "url": "/display_dqm_config"+path,
         "dataSrc": function ( json ) {
                 if(json=="false" || json==false){
                     alert("No DQM Checks Registered!!")
                     return 0
                 }else{
                     return json.dqm_config;
                 }

         }
     },
     "columns": [
         { "data": "qc_id" },
         { "data": "qc_type" },
		 { "data": "table_name" },
         { "data": "column_name" },
           { "data": "param", "defaultContent": ""},
           { "data": "filter_column","defaultContent":""},
            { "data": "active_ind" },
            { "data": "primary_key" },
		 { "data": "criticality" },
		 { "data": "create_by" },
		 { "data": "create_ts" },
		 { "data": "update_by" },
		 { "data": "update_ts" }

     ]
   });


   $('#error_table').dataTable( {
     "ajax": {
         "url": "/display_dqm_error"+path,
         "dataSrc": function ( json ) {
                 if(json=="false" || json==false){
                     alert("NO DQM Errors To Display!!")
                     return 0
                 }else{
                     return json.dqm_error;
                 }

         }
     },
     "columns": [
	     { "data": "run_id" },
         { "data": "file_id" },
         { "data": "qc_id" },
         { "data": "qc_type" },
		 { "data": "table_name" },
         { "data": "column_name" },
		 { "data": "criticality" },
         { "data": "error_value" },
		 { "data": "create_by" },
		 { "data": "create_ts" }
     ]
   });


   $('#load_history_table').dataTable( {
     "ajax": {
         "url": "/display_load_history"+path,
         "dataSrc": function ( json ) {
                 if(json=="false" || json==false){
                     alert("No Process Log To Display!!")
                     return 0
                 }else{
                     return json.load_data;
                 }

         }
     },
     "columns": [
         { "data": "dataset_id" },
         { "data": "dataset_subject_area" },
         { "data": "data_week" },
         { "data": "batch_id" },
         { "data": "file_id" },
           { "data": "pre_landing_record_count" },
           { "data": "post_dqm_record_count"},
            { "data": "dqm_error_count" },
            { "data": "unified_record_count" }

     ]
   });

});
