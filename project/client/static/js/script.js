$(function(){
	$('button').click(function(){
		var jsonPayload = {
			user:$('#txtUsername').val(),
			pass:$('#txtPassword').val()
		}
		$.ajax({
			url: '/signUpRequest',
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
