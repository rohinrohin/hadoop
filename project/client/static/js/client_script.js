$(function(){
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
