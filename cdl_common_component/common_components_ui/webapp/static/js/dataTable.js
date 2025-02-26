$( document ).ready(function() {


    $('#catalog_table').dataTable( {
         "ajax": {
             "url": "/getCatalogData",
             "dataSrc": function ( json ) {
                 if(json=="false" || json==false){
                     alert("No Data Registered , Please Register A Datasource!!")
                     return 0
                 }else{
                     return json.catalogData;
                 }

             }

         },

         "columnDefs": [
             {
                "targets": 4,
                "data": "",

                 "render": function ( data, type, row, meta ) {
                    fileName=row.sample_file_name
                     datasetId = row.dataset_id
                     if(fileName==null||fileName==""||fileName==undefined ){
                         return "<div class=\"dropdown\">\n" +
                                "  <button class='dropbtn'>More Info</button>\n" +
                                "  <div class=\"dropdown-content\">\n" +
                                "    <a class='info' href=\"#\">Data Details</a>\n" +
                                "    <a href="+"'/loadhistory/"+datasetId+"'"+" target='_blank'>Show Load Stats</a>\n" +
                                "    <a href="+"'/dataDqm/"+datasetId+"'"+" target='_blank'>DQM Analysis</a>\n" +
                                "    <a href='http://10.228.32.250:8889' target='_blank'>View Data in Hue</a>\n" +
                                "    <a class='no-file'>Download Sample</a>\n" +
                                "  </div>\n" +
                                "</div>"
                    }else{
                         return  "<div class=\"dropdown\">\n" +
                                "  <button class='dropbtn'>More Info</button>\n" +
                                "  <div class=\"dropdown-content\">\n" +
                                "    <a class='info' href=\"#\">Data Details</a>\n" +
                                "    <a href="+"'/loadhistory/"+datasetId+"'"+" target='_blank'>Show Load Stats</a>\n" +
                                "    <a href="+"'/dataDqm/"+datasetId+"'"+" target='_blank'>DQM Analysis</a>\n" +
                                "    <a href='http://10.228.32.250:8889' target='_blank'>View Data in Hue</a>\n" +
                                "    <a href="+"'/getSourceFile/"+fileName.trim()+"'"+"  target='_blank'>Download Sample</a>\n" +
                                "  </div>\n" +
                                "</div>"
                    }
                }
            },
             {
                 "targets": 3,
                "data": "dataset_latest_dataload_time",
                "render": function ( data, type, row, meta ) {
                     if(data==null||data==""||data==undefined ){
                         return "N/A";
                    }else{
                         return data;
                    }
                }
             }



        ]

        ,"columns": [
             { "data": "dataset_provider" },
             { "data": "dataset_description" },
             { "data": "dataset_subject_area" },
             { "data": "dataset_latest_dataload_time" ,
                "defaultContent": ""}

         ],

        "initComplete": function( settings, json ) {
             $(".dropdown-content .no-file").click(function(){
                alert("No Sample File Was Uploaded!!")
            });
        }



       });

     $('#catalog_table tbody').on('click', '.info', function(){
        var row = $(this).closest('tr');
        var data = $('#catalog_table').dataTable().fnGetData(row);
        console.log(data);
        var dialog = document.querySelector('#sdialog_'+data.dataset_id);

			function toggle() {
				if (dialog.getAttribute('open') == null) {
					dialog.open();
				} else {
					dialog.close();
				}
			}
			if (!dialog) {
				dialog = new zs.dialogElement();
				dialog.addEventListener('configure', function () {
					toggle();
				});

				dialog.classList.add('zs-modal');
				dialog.setAttribute('id', 'sdialog_'+data.dataset_id);

				header='<header><h3>'+data.dataset_name+'</h3></header>'
                div1 = "<section><div style='display:inline;'><i class=\"fa fa-calendar\" style='color:#949494;'></i><span style='font-weight:bold;color:#6e6e6e;margin-left:8px;'>Metadata Created:</span> <span style='color:#7d7d7d;'>"+data.dataset_creation_timestamp+"</span><br clear='all'></div>"
                div2="<div><p>"+data.dataset_description+"</p></div></section>"


                extraInfoStart="<section class='module-content additional-info'><h3>Additional Metadata</h3><table class='TFtable'><tbody>"
                row1="<tr><th scope='row' >dataset_file_format</th>"+"<td>"+data.dataset_file_format+"</td></tr>"
                row2="<tr><th scope='row' >dataset_file_compression_codec</th>"+"<td>"+data.dataset_file_compression_codec+"</td></tr>"
                row3="<tr><th scope='row' >dataset_load_type</th>"+"<td>"+data.dataset_load_type+"</td></tr>"
                row4="<tr><th scope='row' >dataset_expiry_date</th>"+"<td>"+data.dataset_expiry_date+"</td></tr>"
                row5="<tr><th scope='row' >dataset_arrivals_frequency</th>"+"<td>"+data.dataset_arrivals_frequency+"</td></tr>"

                extraInfoEnd="</tbody></table></section>"

				dialog.innerHTML = header+div1+div2/*+div3+tagStart+tagEnd*/+extraInfoStart+row1+row2+row3+row4+row5+extraInfoEnd;

				document.body.appendChild(dialog);
			} else {
				toggle();
			}
    });



     $('#searchES').on('click', function(){
        searchCriteria= $("#searchESIndex").val();

        if(searchCriteria == "" || searchCriteria == undefined || searchCriteria == null){
            alert("Please Enter Search Criteria!!")
            return 0
        }else{
            $('#myTable').DataTable().destroy();
            $('#catalog_table').dataTable( {
                destroy: true,
                 "ajax": {
                     "url": "/getCatalogSearchData",
                     data:{"searchCriteria":searchCriteria},
                     type: 'POST',
                     "dataSrc": function ( json ) {
                         if(json=="false" || json == false){
                             alert("No Data Registered With Search Criteria!!")
                             return 0
                         }else{
                             return json.catalogData;
                         }
                     }
                 },
                "columnDefs": [
                 {
                    "targets": 4,
                    "data": "",

                     "render": function ( data, type, row, meta ) {
                        fileName=row.sample_file_name
                        datasetId=row.dataset_id
                         if(fileName==null||fileName==""||fileName==undefined ){
                             return "<div class=\"dropdown\">\n" +
                                    "  <button class='dropbtn'>More Info</button>\n" +
                                    "  <div class=\"dropdown-content\">\n" +
                                    "    <a class='info' href=\"#\">Data Details</a>\n" +
                                    "    <a href="+"'/loadhistory/"+datasetId+"'"+" target='_blank'>Show Load Stats</a>\n" +
                                    "    <a href="+"'/dataDqm/"+datasetId+"'"+" target='_blank'>DQM Analysis</a>\n" +
                                    "    <a href='http://10.228.32.250:8889' target='_blank'>View Data in Hue</a>\n" +
                                    "    <a class='no-file'>Download Sample</a>\n" +
                                    "  </div>\n" +
                                    "</div>"
                        }else{
                             return  "<div class=\"dropdown\">\n" +
                                    "  <button class='dropbtn'>More Info</button>\n" +
                                    "  <div class=\"dropdown-content\">\n" +
                                    "    <a class='info' href=\"#\">Data Details</a>\n" +
                                    "    <a href="+"'/loadhistory/"+datasetId+"'"+" target='_blank'>Show Load Stats</a>\n" +
                                    "    <a href="+"'/dataDqm/"+datasetId+"'"+" target='_blank'>DQM Analysis</a>\n" +
                                    "    <a href='http://10.228.32.250:8889' target='_blank'>View Data in Hue</a>\n" +
                                    "    <a href="+"'/getSourceFile/"+fileName.trim()+"'"+"  target='_blank'>Download Sample</a>\n" +
                                    "  </div>\n" +
                                    "</div>"
                        }
                    }
                },
                 {
                     "targets": 3,
                    "data": "dataset_latest_dataload_time",
                    "render": function ( data, type, row, meta ) {
                         if(data==null||data==""||data==undefined ){
                             return "N/A";
                        }else{
                             return data;
                        }
                    }
                 }
            ]

            ,"columns": [
                 { "data": "dataset_provider" },
                 { "data": "dataset_description" },
                 { "data": "dataset_subject_area" },
                 { "data": "dataset_latest_dataload_time" ,
                    "defaultContent": ""}

             ],

            "initComplete": function( settings, json ) {
                 $(".dropdown-content .no-file").click(function(){
                    alert("No Sample File Was Uploaded!!")
                });
            }

               });

                }

     });




});
