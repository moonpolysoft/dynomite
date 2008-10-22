var DrawArrows = {
  arrow: [
      [ 2, 0 ],
      [ -10, -4 ],
      [ -10, 4]
  ],
  
  rotateShape: function(shape,ang) {
      var rv = [];
      for(p in shape)
          rv.push(DrawArrows.rotatePoint(ang,shape[p][0],shape[p][1]));
      return rv;
  },

  rotatePoint: function(ang,x,y) {
      return [
          (x * Math.cos(ang)) - (y * Math.sin(ang)),
          (x * Math.sin(ang)) + (y * Math.cos(ang))
      ];
  },

  translateShape: function(shape,x,y) {
      var rv = [];
      for(p in shape)
          rv.push([ shape[p][0] + x, shape[p][1] + y ]);
      return rv;
  },
  
  enable: function(ctx) {
    
    ctx.drawFilledPolygon = function(shape) {
        this.beginPath();
        this.moveTo(shape[0][0],shape[0][1]);

        for(p in shape)
            if (p > 0) this.lineTo(shape[p][0],shape[p][1]);

        this.lineTo(shape[0][0],shape[0][1]);
        this.fill();
    };

    ctx.drawLineArrow = function(x1,y1,x2,y2) {
        this.beginPath();
        this.moveTo(x1,y1);
        this.lineTo(x2,y2);
        this.stroke();
        var ang = Math.atan2(y2-y1,x2-x1);
        this.drawFilledPolygon(DrawArrows.translateShape(DrawArrows.rotateShape(DrawArrows.arrow,ang),x2,y2));
    };
  }
  
  
};
