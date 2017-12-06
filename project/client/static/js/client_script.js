$(function(){
	function changeIndicator(color, status){
		clusterIndicator = document.getElementById("clusterIndicator");
		clusterIndicator.innerHTML = "Cluster is "+status;
		var canvas = document.getElementById("circlecanvas");
		var context = canvas.getContext("2d");
		context.arc(25, 25, 15, 0, Math.PI * 2, false);
		context.fillStyle = color;
		context.fill()
	}
	changeIndicator("white", "INITIALIZING");
	function getStatus(){
		$.ajax({
			url: '/status',
			type: 'POST',
			success: function(status){
				console.log(status);
				if(status == "STABLE")
					changeIndicator("green", status);
				else if(status == "UNSTABLE")
					changeIndicator("red", status);
				setTimeout(getStatus, 5000);
			},
			error: function(err){
				console.log(err);
			}
		});
	}
	setTimeout(getStatus, 5000);
	$('#put-button').click(function(){
		var jsonPayload = {
			type:'put',
			key:$('#put_key').val(),
			value:$('#put_value').val()
        }
        $.ajax({
			url: '/request',
			data: JSON.stringify(jsonPayload),
			type: 'POST',
			dataType: 'JSON',
			success: function(response){
				console.log(response);
				$("#response-col").html("<div>Status: "+ response['status']+"</div><div>Key: "+ jsonPayload.key+"</div>");

                var x= $("#logs")
				for(var i=0; i<$(response['logger']).length; i++)
				{
					x.val(x.val()+"\n"+response['logger'][i]);
				}
			var textarea=document.getElementById('logs');
			textarea.scrollTop=textarea.scrollHeight;

			},
			error: function(error){
				console.log(error);
			}
		});
		});
	$('#get-button').click(function(){
		var jsonPayload = {
			type: 'get',
			key:$('#get_key').val()
			}
			console.log(jsonPayload.key);
		$.ajax({
			url: '/request',
			data: JSON.stringify(jsonPayload),
			type: 'POST',
			dataType: 'JSON',

			success: function(response){
				console.log(response);
				$("#response-col").html("<div>Status: "+ response['status']+"</div><div>Key: "+ jsonPayload.key +"</div><div>Value:     "+response['data'] + "</div>");

            var x= $("#logs")
				for(var i=0; i<$(response['logger']).length; i++)
				{
					x.val(x.val()+"\n"+response['logger'][i]);
				}
				var textarea=document.getElementById('logs');
				textarea.scrollTop=textarea.scrollHeight;

			},
			error: function(error){
				console.log(error);
			}
		});
	});
});
