function hsv2rgb(h, s, v) {
  var hi = Math.floor(h/60) % 6;
  var f = h / 60 - Math.floor(h/60);
  var p = Math.round(255 * v * (1-s));
  var q = Math.round(255 * v * (1-f*s));
  var t = Math.round(255 * v * (1-(1-f)*s));
  v = Math.round(v * 255);
  switch (hi) {
    case 0: return [v,t,p];
    case 1: return [q,v,p];
    case 2: return [p,v,t];
    case 3: return [p,q,v];
    case 4: return [t,p,v];
    case 5: return [v,p,q];
  }
}

function hsv2rgba(h, s, v) {
  var color = hsv2rgb(h, s, v);
  return "rgba(" + color[0] + "," + color[1] + "," + color[2] + ",1)";
}