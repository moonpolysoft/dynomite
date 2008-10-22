

function node_index(member_nodes, node_name) {
  var index = -1; 
  $.each(member_nodes, function(n, node) {
    if (node_name == node.name) {
      index = n;
    }
  });
  return index;
}

function draw_nodes(member_nodes, running_nodes, canvas) {
  var ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  CanvasTextFunctions.enable(ctx);
  DrawArrows.enable(ctx);
  var xoffset = canvas.width / 2;
  var yoffset = canvas.height / 2;
  var scale = xoffset - 50;
  
  //rotate first node to the top
  var angular_offset = Math.PI / 2 * 3;
  
  var angle = 2 * Math.PI / member_nodes.length;
  
  $.each(member_nodes, function(n, node) {
    var beginTheta = n * angle + angular_offset;
    $.each(node.replicas, function(i, replica) {
      if ($.inArray(replica, running_nodes) != -1 && replica != node.name) {
        var index = node_index(member_nodes, replica);
        var endTheta = index * angle + angular_offset;
        // console.log([node.name, replica, index, beginTheta, endTheta]);
        // ctx.beginPath();
        ctx.strokeStyle = "#193E4A";
        ctx.fillStyle = "#193E4A";
        ctx.drawLineArrow(
          Math.cos(beginTheta) * scale + xoffset - 2.5, 
          Math.sin(beginTheta) * scale + yoffset,
          Math.cos(endTheta) * scale + xoffset - 2.5,
          Math.sin(endTheta) * scale + yoffset);
      }
    });
  });
  $.each(member_nodes, function(n, node) {
    ctx.beginPath();
    ctx.strokeStyle = "#000000"
  
    var theta = n * angle + angular_offset;
    if ($.inArray(node.name, running_nodes) == -1) {
      ctx.fillStyle = "#ff0000"
    } else {
      ctx.fillStyle = "#00ff00"
    }
    var x = Math.cos(theta) * scale + xoffset;
    var y = Math.sin(theta) * scale + yoffset;
    // ctx.moveTo(x, y);
    ctx.arc(x-5,y,5,0,Math.PI*2,true);
    ctx.fill();
    ctx.stroke();
    ctx.drawTextCenter("sans", 10, x, y-15, node.name);
  });
  
  
}