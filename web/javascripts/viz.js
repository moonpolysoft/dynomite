function decimals(number, value) {
  with(Math) {
    var coefficient = pow(10, number);
    return round(value * coefficient) / coefficient;
  }
}

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
          Math.cos(beginTheta) * scale + xoffset, 
          Math.sin(beginTheta) * scale + yoffset,
          Math.cos(endTheta) * scale + xoffset,
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
    ctx.arc(x,y,5,0,Math.PI*2,true);
    ctx.fill();
    ctx.stroke();
    ctx.drawTextCenter("sans", 10, x, y-15, node.name);
  }); 
}

function pickColors(nodes) {
  var angle = 360 / nodes.length;
  var hash = new Array();
  $.each(nodes, function(n, node) {
    hash[node] = n*angle;
  });
  return hash;
}

function partMap(member_nodes) {
  var hash = new Array();
  $.each(member_nodes, function(n, node) {
    hash[node.name] = node.partitions;
  });
  return hash;
}

function replicaMap(member_nodes) {
  var hash = new Array();
  $.each(member_nodes, function(n, node) {
    hash[node.name] = node.replicas;
  });
  return hash;
}

function syncMap(syncs) {
  var hash = new Array();
  $.each(syncs, function(n, sync) {
    hash[sync.partition + ""] = sync.nodes;
  });
  return hash;
}

function drawArcSection(ctx,x,y,radius,width,startAngle,endAngle,unfilled,closed) {
  ctx.beginPath();
  ctx.arc(x,y,radius,startAngle,endAngle,false);
  ctx.lineTo(Math.cos(endAngle) * (radius-width) + x, Math.sin(endAngle) * (radius-width) + y);
  ctx.arc(x,y,radius-width,endAngle,startAngle,true);
  if (closed) {
    ctx.lineTo(Math.cos(startAngle) * radius + x, Math.sin(startAngle) * radius + y);
  }
  if (!unfilled) {
    ctx.fill();
  }
  ctx.stroke();
}

function drawPartitions(nodes, partitions, member_nodes, canvas) {
  var ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  CanvasTextFunctions.enable(ctx);
  
  var xoffset = canvas.width / 2 + 100;
  var yoffset = canvas.height / 2;
  var radius = yoffset - 50;
  
  var angular_offset = Math.PI / 2 * 3;
  var angle = 2 * Math.PI / partitions.length;
  var colormap = pickColors(nodes);
  var partmap = partMap(member_nodes);
  
  $.each(nodes, function(n, node) {
    ctx.fillStyle = hsv2rgba(colormap[node], 1, 1);
    ctx.strokeStyle = "#000";
    ctx.fillRect(5, n*20 + 15, 30, 15);
    ctx.strokeRect(5, n*20 + 15, 30, 15);
    ctx.drawText('sans', 12, 45, n*20 + 27, node // + " " + partmap[node].length
    );
  });
  
  $.each(partitions, function(n, part) {
    ctx.beginPath();
    ctx.strokeStyle = "rgba(0, 0, 0, 1)"
    ctx.fillStyle = hsv2rgba(colormap[part.node], 1, 1);
    var startAngle = n * angle + angular_offset;
    var endAngle = (n+1) * angle + angular_offset;
    drawArcSection(ctx,xoffset,yoffset,radius,20,startAngle,endAngle);
  });
}

function drawRates(node_rates, canvas) {
  var ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  CanvasTextFunctions.enable(ctx);
  
  var xoffset = canvas.width / 2 + 100;
  var yoffset = canvas.height / 2;
  var radius = yoffset - 50;
  
  var angular_offset = Math.PI / 2 * 3;
  var colormap = pickColors($.map(node_rates, function(node) { return node.name }));
  
  var totalLoad = 0;
  $.each(node_rates, function(n, node) { totalLoad += node.get_rate + node.put_rate });
  
  $.each(node_rates, function(n, node) {
    ctx.fillStyle = hsv2rgba(colormap[node.name], 1, 1);
    ctx.strokeStyle = "#000";
    ctx.fillRect(5, n*20 + 15, 30, 15);
    ctx.strokeRect(5, n*20 + 15, 30, 15);
    if (totalLoad == 0) {
      var percentage = "0%"
    } else {
      var percentage = decimals(1, (node.get_rate + node.put_rate) / totalLoad * 100) + "%"
    }
    
    ctx.drawText('sans', 12, 45, n*20 + 27, node.name + " " + percentage); // + " " + partmap[node].length);
  });
  
  
  $.each(node_rates, function(n, node) {
    if (totalLoad == 0) {
      var angle = 2 * Math.PI / node_rates.length;
    } else {
      var angle = 2 * Math.PI * ((node.get_rate + node.put_rate) / totalLoad);
    }
    ctx.fillStyle = hsv2rgba(colormap[node.name], 1, 1);
    drawArcSection(ctx,xoffset,yoffset,radius,20,angular_offset,angular_offset+angle);
    angular_offset += angle;
  });
}

function drawSync(nodes, partitions, member_nodes, syncs_running, diff_sizes, canvas) {
  var ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  CanvasTextFunctions.enable(ctx);
  
  var xoffset = canvas.width / 2 + 100;
  var yoffset = canvas.height / 2;
  var radius = yoffset - 50;
  
  var angular_offset = Math.PI / 2 * 3;
  var angle = 2 * Math.PI / partitions.length;
  var colormap = pickColors(nodes);
  var partmap = partMap(member_nodes);
  var replicamap = replicaMap(member_nodes);
  var syncmap = syncMap(syncs_running);
  
  $.each(nodes, function(n, node) {
    ctx.fillStyle = hsv2rgba(colormap[node], 1, 1);
    ctx.strokeStyle = "#000";
    ctx.fillRect(5, n*20 + 15, 30, 15);
    ctx.strokeRect(5, n*20 + 15, 30, 15);
    ctx.drawText('sans', 12, 45, n*20 + 27, node); // + " " + partmap[node].length);
  });
  
  $.each(partitions, function(n, part) {
    ctx.beginPath();
    ctx.strokeStyle = "rgba(0,0,0,1)"
    ctx.fillStyle = hsv2rgba(colormap[part.node], 1, 1);

    var startAngle = n * angle + angular_offset;
    var endAngle = (n+1) * angle + angular_offset;
    drawArcSection(ctx,xoffset,yoffset,radius,20,startAngle,endAngle);
    
    $.each(replicamap[part.node], function(i, replica) {
      if (replica == part.node) {
        return;
      } 
      
      if (diff_sizes[part.partition + ""] && diff_sizes[part.partition + ""][replica]) {
        diff = diff_sizes[part.partition + ""][replica];
        if (diff == 0) {
          var value = 1;
        } else {
          var value = 1 - Math.log(diff) / Math.log(2) / 32;
        }
        ctx.strokeStyle = "rgba(0, 0, 0, 1)"
        ctx.fillStyle = hsv2rgba(colormap[replica], 1, value);
      } else {
        ctx.strokeStyle = "rgba(0, 0, 0, 1)"
        ctx.fillStyle = hsv2rgba(colormap[replica], 1, 1);
      }
      // ctx.fillStyle = colormap[replica];
      drawArcSection(ctx,xoffset,yoffset,radius-(i*25),20,startAngle,endAngle);
    });
  });
  
  $.each(partitions, function(n, part) {
    var startAngle = n * angle + angular_offset;
    var endAngle = (n+1) * angle + angular_offset;
    
    if (syncmap[part.partition + ""] && $.inArray(part.node, syncmap[part.partition + ""]) != -1) {
      // ctx.strokeStyle = "#ffffff";
      ctx.lineWidth = 3;
      drawArcSection(ctx,xoffset,yoffset,radius,20,startAngle,endAngle,true,true);
    } else {
      return;
    }
    
    $.each(replicamap[part.node], function(i, replica) {
      if (replica == part.node) {
        return;
      } 
      if (syncmap[part.partition + ""] && $.inArray(replica, syncmap[part.partition + ""]) != -1) {
        // ctx.strokeStyle = "#ffffff";
        ctx.lineWidth = 2;
        for(x=0;x<5;x++) {drawArcSection(ctx,xoffset,yoffset,radius-(i*25),20,startAngle,endAngle,true,true);}
      }
    });
    ctx.lineWidth = 1;
  });
}