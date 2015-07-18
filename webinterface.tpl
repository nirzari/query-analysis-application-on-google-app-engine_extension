<html>
<head>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.4/jquery.min.js"></script>
<script type="text/javascript">
$( document ).ready(function() {
	console.log("Tu to run ho ja plzzz");
	$("#button").click(function(){
                console.log("Hiiiiiii");
		var dataobj = {"param1": $("#param1").val(), "mag": $("#mag").val(), "param2": $("#param2").val(), "loc":$("#loc").val()};
		if ( ranges(parseInt(dataobj['mag']),1,10)){ 
			console.log("Hello");
    			$.ajax({
				type: "POST",
        			url: "/dynamic_query",
        			data: dataobj,
        			success: function(query_ans){
					console.log(query_ans);
					$("#result").html(query_ans);
        			}
			});
		}
        	else{
	        	$("#result").text("Enter valid magnitude");
		}
	});
	function ranges(x, min, max){
    		return x >= min && x <= max;
    	}
});
</script>
</head>
<body>
Enter magnitude: <select name = "param1" id = "param1">
  <option value="eq">Equals</option>
  <option value="gt">Greater than</option>
  <option value="gte">Greater than or Equals</option>
  <option value="lt">less than</option>
  <option value="lte">less than or equals</option>
  </select>
  <input type="text" name="magnitude" id = "mag">
</br>
&nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp<select name="param2" id="param2">
  <option value = "or">OR</option>
  <option value = "and">AND</option>
  </select>
<br>
Enter Location: &nbsp <input type= "Text" name = "location" id = "loc">
<input type="button" id="button" value="Fire Query">
<div id = "result">
</div>
</body>
</html>
