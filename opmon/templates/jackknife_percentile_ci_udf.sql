/*
Calculates a confidence interval using a jackknife resampling technique
for a given percentile of a histogram. See
https://en.wikipedia.org/wiki/Jackknife_resampling
Users must specify the percentile of interest as the first parameter
and a histogram struct as the second.
*/
CREATE TEMP FUNCTION jackknife_percentile_ci(
  percentiles ARRAY<INT64>,
  histogram STRUCT<values ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>,
  metric STRING
)
RETURNS ARRAY<STRUCT<
    metric STRING,
    statistic STRING,
    point FLOAT64,
    lower FLOAT64,
    upper FLOAT64,
    parameter STRING
>> DETERMINISTIC
LANGUAGE js
AS
"""
  function computePercentile(percentile, histogram) {
    if (percentile < 0 || percentile > 100) {
      throw "percentile must be a value between 0 and 100";
    }
    let values = histogram.map(bucket => parseFloat(bucket.value));
    let total = values.reduce((a, b) => a + b, 0);
    let normalized = values.map(value => value / total);
    // Find the index into the cumulative distribution function that corresponds
    // to the percentile. This undershoots the true value of the percentile.
    let acc = 0;
    let index = null;
    for (let i = 0; i < normalized.length; i++) {
      acc += normalized[i];
      index = i;
      if (acc >= parseFloat(percentile) / 100) {
        break;
      }
    }
    // NOTE: we do not perform geometric or linear interpolation, but this would
    // be the place to implement it.
    if (histogram.length == 0) {
      return null;
    }
    return histogram[index].key;
  }
  function erfinv(x){
      var z;
      var a = 0.147;
      var the_sign_of_x;
      if(0==x) {
          the_sign_of_x = 0;
      } else if(x>0){
          the_sign_of_x = 1;
      } else {
          the_sign_of_x = -1;
      }
      if(0 != x) {
          var ln_1minus_x_sqrd = Math.log(1-x*x);
          var ln_1minusxx_by_a = ln_1minus_x_sqrd / a;
          var ln_1minusxx_by_2 = ln_1minus_x_sqrd / 2;
          var ln_etc_by2_plus2 = ln_1minusxx_by_2 + (2/(Math.PI * a));
          var first_sqrt = Math.sqrt((ln_etc_by2_plus2*ln_etc_by2_plus2)-ln_1minusxx_by_a);
          var second_sqrt = Math.sqrt(first_sqrt - ln_etc_by2_plus2);
          z = second_sqrt * the_sign_of_x;
      } else { // x is zero
          z = 0;
      }
      return z;
  }
  function array_sum(arr) {
      return arr.reduce(function (acc, x) {
          return acc + x;
      }, 0);
  }
  function array_avg(arr) {
      return array_sum(arr) / arr.length;
  }
  function getMeanErrorsPercentile(percentile, histogram, fullDataPercentile) {
    var jk_percentiles = [];
    histogram.values.forEach((bucket, i) => {
      var histCopy = JSON.parse(JSON.stringify(histogram.values));
      histCopy[i].value--;
      jk_percentiles.push(computePercentile(percentile, histCopy));
    });
    return jk_percentiles.map(x => Math.pow(x - fullDataPercentile, 2));
  }
  function percentiles_with_ci(percentiles, histogram, metric) {
    var results = [];
    for (var i = 0; i < percentiles.length; i++) {
        percentile = percentiles[i];
        if (!histogram.values || !histogram ) {
            results.push({
                "metric": metric,
                "statistic": "percentile",
                "lower": null,
                "upper": null,
                "point": null,
                "parameter": percentile,
            });
            continue;
        }
        
        var fullDataPercentile = parseFloat(computePercentile(percentile, histogram.values));
        var meanErrors = getMeanErrorsPercentile(percentile, histogram, fullDataPercentile);
        var count = histogram.values.reduce((acc, curr) => acc + parseFloat(curr.value), 0);
        var std_err = Math.sqrt((count - 1) * array_avg(meanErrors));
        var z_score = Math.sqrt(2.0) * erfinv(0.90);
        var hi = fullDataPercentile + (z_score * std_err);
        var lo = fullDataPercentile - (z_score * std_err);
        results.push({
            "metric": metric,
            "statistic": "percentile",
            "lower": lo.toFixed(2),
            "upper": hi.toFixed(2),
            "point": fullDataPercentile.toFixed(2),
            "parameter": percentile,
        });
    }
    return results;
  }
  return percentiles_with_ci(percentiles, histogram, metric);
  """;
