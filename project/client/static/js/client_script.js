$(function(){
	$('#put-button').click(function(){
		var jsonPayload = {
			key:$('#put_key').val(),
			value:$('#put_value').val()
        }
        alert(key)
        alert(value)
		$.ajax({
			url: '/request',
			data: JSON.stringify(jsonPayload),
			type: 'POST',
			dataType: 'JSON',
			success: function(response){
				console.log(response);
			},
			error: function(error){
				console.log(error);
			}
		});
	});
});
