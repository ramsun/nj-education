  var url = `/samples/${sample}`;
  d3.json(url).then(function(response) {

    var tenSamples = response.sample_values.slice(0, 10);
    var tenID = response.otu_ids.slice(0, 10);
    var tenLabels = response.otu_labels.slice(0, 10);


    var pieTrace = {
      values: tenID,
      labels: tenSamples,
      type: 'pie',
      hoverinfo: tenLabels
    };
    var bubbleTrace = {
      x: tenID,
      y: tenSamples,
      mode: 'markers',
      text: tenLabels,
      marker: {
        size: tenSamples,
        color: tenID,
      }
    };
    pie = [pieTrace];
    bubble = [bubbleTrace];


    Plotly.newPlot("pie", pie);
    Plotly.newPlot("bubble", bubble);
  });

}