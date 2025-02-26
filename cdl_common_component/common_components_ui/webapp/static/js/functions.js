$( document ).ready(function() {
    $("#submit-data-form").hide();
    $("#schmea-inference-table").hide();
    $("#upload-file").hide();
    $("#schema-selection").hide();
    $("#add-rows").hide();
    var i=0;

    $("#add-rows").click(function() {

        var markup = "<tr><td><input name=\"column_name[]\" type=\"text\" class=\"zs-input\"></td><td>"+
                        "<SELECT name=\"column_datatype[]\"> "+
                        "<OPTION selected='selected' value='String'>String</OPTION><OPTION value='date'>Date</OPTION><OPTION value='timestamp'>TimeStamp</OPTION> <OPTION value='char'>Boolean</OPTION><OPTION value='int'>Integer</OPTION></SELECT>"
                        +"</td>"+
                        "<td><input name=\"column_comment[]\" type=\"text\" class=\"zs-input\"></td>"+
                        "<td><input  name=\"column_sequence[]\" value="+(i+1)+"></td>"+
                        "<td><input name=\"tags_column[]\" type=\"text\" class=\"zs-input\"></td>"+
                        "<td><i class=\"delete-row fa fa-trash-o\"></i></td>"
                        "</tr>";
                    $("#schmea-inference-table >tbody").append(markup);
                    i++;


    });

$(document).on('click','.delete-row',function() {
     $(this).closest('tr').remove();
     i--;
});

    $("#schmea-inference-table >tbody >tr").on("click", "td.delete-row", function(){
        $(this).closest('tr').remove();
    });

    $("#schema-inference").change(function() {
        var value  = $(this).val();
        $("#schmea-inference-table > tbody").empty();
        if(value == "upload"){
            i=0;
            $("#add-rows").hide();
            $("#upload-file").show();
            $("#schmea-inference-table").hide();
        }else if(value == "manual"){
            $("#add-rows").show();
            $("#upload-file").hide();
            $("#schmea-inference-table").show();
        }else{
            return false;
        }
    });

   $("#tab1").click(function() {
       $("#submit-data-form").hide();
       $("#upload-file").hide();
       $("#schema-selection").hide();
       $("#tab1").attr("active", "");
       $("#tab2").removeAttr("active");
       $("#tab3").removeAttr("active");
       $("#tab4").removeAttr("active");

       $("#set1").removeClass().addClass("show-fieldset");
       $("#set2").removeClass().addClass("hide-fieldset");
       $("#set3").removeClass().addClass("hide-fieldset");
       $("#set4").removeClass().addClass("hide-fieldset");
   });


   $("#tab2").click(function() {
       $("#submit-data-form").hide();
       $("#upload-file").hide();
       $("#schema-selection").hide();
       $("#tab2").attr("active", "");
       $("#tab1").removeAttr("active");
       $("#tab3").removeAttr("active");
       $("#tab4").removeAttr("active");

       $("#set2").removeClass().addClass("show-fieldset");
       $("#set1").removeClass().addClass("hide-fieldset");
       $("#set3").removeClass().addClass("hide-fieldset");
       $("#set4").removeClass().addClass("hide-fieldset");
   });

   $("#tab3").click(function() {
       $("#submit-data-form").hide();
       /*$("#upload-file").show();*/
       $("#schema-selection").show();
       $("#tab3").attr("active", "");
       $("#tab1").removeAttr("active");
       $("#tab2").removeAttr("active");
       $("#tab4").removeAttr("active");

       $("#set3").removeClass().addClass("show-fieldset");
       $("#set1").removeClass().addClass("hide-fieldset");
       $("#set2").removeClass().addClass("hide-fieldset");
       $("#set4").removeClass().addClass("hide-fieldset");
   });

   $("#tab4").click(function() {
       $("#submit-data-form").show();
       /*$("#upload-file").show();*/
       $("#schema-selection").show();
       $("#tab4").attr("active", "");
       $("#tab1").removeAttr("active");
       $("#tab2").removeAttr("active");
       $("#tab3").removeAttr("active");

       $("#set1").removeClass().addClass("fieldsetblock").addClass("filedset-3");
       $("#set2").removeClass().addClass("fieldsetblock").addClass("filedset-3");
       $("#set3").removeClass().addClass("fieldsetblock").addClass("filedset-3");
   });



    $(".datetimepicker").datetimepicker(
        {
            datepicker: false,
            format: 'H:i',
            step: 30,
            defaultTime:'12:00',
        }
    );

    $(".expirationtime").datetimepicker(
        {
            timepicker:false,
            format:'Y-m-d'
        }
    );
    /*$("#submit-data-form").click(function() {
           $("#data-register-form").submit();
       });*/



    $("#dataset_arrivals_frequency").change(function () {

        //alert($(this).find(":selected").val());
       /* $("#dataset_dataprovider_sla").append("<spam id='daily_selector'  name='daily_selector' class='spam_hidden'>\n" +
            "                 <br>\n" +
            "                         <input type='text' placeholder='Select Time' class='datetimepicker topmargin-1'>\n" +
            "                </spam>");*/
        if($(this).find(":selected").val()=="Daily"){
			$("#daily_selector").removeClass("spam_hidden");
			$("#weekly_selector").removeClass("spam_hidden").addClass("spam_hidden");
			$("#monthly_selector").removeClass("spam_hidden").addClass("spam_hidden");
		}else if($(this).find(":selected").val() == "Weekly"){
			$("#weekly_selector").removeClass("spam_hidden");
			$("#daily_selector").removeClass("spam_hidden").addClass("spam_hidden");
			$("#monthly_selector").removeClass("spam_hidden").addClass("spam_hidden");
		}else if($(this).find(":selected").val()=="Monthly"){
			$("#monthly_selector").removeClass("spam_hidden");
			$("#weekly_selector").removeClass("spam_hidden").addClass("spam_hidden");
			$("#daily_selector").removeClass("spam_hidden").addClass("spam_hidden");
		}else{
			$("#daily_selector").removeClass("spam_hidden").addClass("spam_hidden");
			$("#weekly_selector").removeClass("spam_hidden").addClass("spam_hidden");
			$("#monthly_selector").removeClass("spam_hidden").addClass("spam_hidden");
		}
    });

    $('#upload-file-btn').click(function() {
        var form_data = new FormData($("#upload-file")[0]);
        $.ajax({
            type: 'POST',
            url: '/getfile',
            data: form_data,
            contentType: false,
            cache: false,
            processData: false,
            async: false,
            success: function (data) {
                console.log(data);
                data = data.replace("{","");
                data = data.replace("}","");
                if(data.length==0){
                    $("#schmea-inference-table").hide();
                    $("#submit-data-form").hide();
                    alert("Invalid File Format");
                }else{
                    $("#schmea-inference-table >tbody").empty();
                    $("#schmea-inference-table").show();

                    data = data.replace(/\"/g, "");

                    infer_col = data.split(",",-1);
                    for(var i=0;i<infer_col.length;i++){
                    console.log(infer_col[i].split(":",-1)[0]);
                    var markup = "<tr><td name='column_name[]'>"+infer_col[i].split(":",-1)[0]+"</td><td>"+
                        "<SELECT name=\"column_datatype[]\"> <option selected='selected'>"+infer_col[i].split(":",-1)[1]+"</option>"+
                        "<OPTION value='String'>String</OPTION><OPTION value='date'>Date</OPTION><OPTION value='timestamp'>TimeStamp</OPTION> <OPTION value='char'>Boolean</OPTION><OPTION value='int'>Integer</OPTION></SELECT>"
                        +"</td>"+
                        "<td><input name=\"column_comment[]\" type=\"text\" class=\"zs-input\"></td>"+
                        "<td><input  name=\"column_sequence[]\" value="+(i+1)+"></td>"+
                        "<td><input name=\"tags_column[]\" type=\"text\" class=\"zs-input\"></td>"+
                        "</tr>";
                    $("#schmea-inference-table >tbody").append(markup);

                }
                }


            },error: function () {
                alert("Invalid File Format");
                $("#schmea-inference-table >tbody").empty();
                $("#schmea-inference-table").hide();
            }
        })
    });

    $("#data-register-form").on("submit", function (event) {
        event.preventDefault();
        var freq = $("#dataset_arrivals_frequency").find(":selected").val();
        if(freq=="Daily"){
            dataset_dataprovider_sla=$("#daily_selector_time").val();
		}else if(freq == "Weekly"){
            dataset_dataprovider_sla=$("#weekly_selector_day").val() + "|" +$("#weekly_selector_time").val();
		}else if(freq=="Monthly"){
            dataset_dataprovider_sla=$("#monthly_selector_week").val() + "|" +$("#monthly_selector_day").val()+"|"+$("#monthly_selector_time").val();
		}else{
            dataset_dataprovider_sla="12:00";
		}

        var tableLength = $("#schmea-inference-table tbody tr").length;

        if(tableLength ==0){
		    alert("Please upload sample file or Create Schema Manually!!")
            return 0;
        }else{
            var column_name =[];
            var column_datatype =[];
            var column_comment =[];
            var column_sequence =[];
            var tags_column = [];
            var schema_info = [];

            for(var i=0;i<tableLength;i++){
               column_name[i] =  $("[name='column_name[]']")[i].innerText;
               column_datatype[i] = $("[name='column_datatype[]'] :selected")[i].innerText;
               column_comment[i] = $("[name='column_comment[]']")[i].value;
               column_sequence[i] =  $("[name='column_sequence[]']")[i].value;
               tags_column[i] = $("[name='tags_column[]']")[i].value.replace(",","|");
            }

            schema_info = [column_name,column_datatype,column_comment,column_sequence,tags_column];

        }
        var fullPath =$("input[name='file']").val();
        if(fullPath == null || fullPath==""){
            fileName="";
        }else{
            var startIndex = (fullPath.indexOf('\\') >= 0 ? fullPath.lastIndexOf('\\') : fullPath.lastIndexOf('/'));
            var fileName = fullPath.substring(startIndex);
            if (fileName.indexOf('\\') === 0 || fileName.indexOf('/') === 0) {
                fileName = fileName.substring(1);
            }
        }

        $.ajax({
                url: '/submitFormData',
                data: {'data': decodeURIComponent($('form').serialize()),'dataset_dataprovider_sla':dataset_dataprovider_sla,'schema_info':decodeURIComponent(JSON.stringify(schema_info)),'file_name':fileName},
                type: 'POST',
                dataType: 'json',
                success: function(response){
                    if(data = "true"){
                        alert("Data Source Registered,Redirecting to Search Catalog")
                        //top.location.href = '/dataCatalog';
                    }else{
                        alert("Something went wrong . Please try again")
                        //top.location.href = '/dataCatalog';
                    }
                },
                error: function(error){
                    alert("Something went wrong . Please try again")
                    //top.location.href = '/dataCatalog';
                }
            });
    });

});