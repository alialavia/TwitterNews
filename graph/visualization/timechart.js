function drawChart(data, elem)
{
  var HEIGHT = $(elem).height();
  var WIDTH = $(elem).width();
d3chart.select(elem).html("")
var margin = {top: 20, right: 20, bottom: 30, left: 50},
 height = HEIGHT - margin.top - margin.bottom, width = WIDTH - margin.left - margin.right;

var x = d3chart.time.scale()
    .range([0, width]);

var y = d3chart.scale.linear()
    .range([height, 0]);

var xAxis = d3chart.svg.axis()
    .scale(x)
    .orient("bottom");

var yAxis = d3chart.svg.axis()
    .scale(y)
    .orient("left");

var line = d3chart.svg.line()
    .x(function(d) { return x(d.date); })
    .y(function(d) { return y(d.freq); });

var svg = d3chart.select(elem).append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
    
x.domain(d3chart.extent(data, function(d) { return d.date; }));
y.domain(d3chart.extent(data, function(d) { return d.freq; }));

svg.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0," + height + ")")
    .call(xAxis);

svg.append("g")
    .attr("class", "y axis")
    .call(yAxis)
  .append("text")
    .attr("transform", "rotate(-90)")
    .attr("y", 6)
    .attr("dy", ".71em")
    .style("text-anchor", "end")
    .text("Term frequency");

svg.append("path")
    .datum(data)
    .attr("class", "line")
    .attr("d", line);
}