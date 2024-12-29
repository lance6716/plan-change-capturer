package report

const tpl = `
<!DOCTYPE html>
<html>
 <head>
  <meta charset="UTF-8">
  <title>Plan Change Capturer Report</title>
 </head>
 <body>
  <h1>Plan Change Capturer Report</h1>
  <h2>Task Information:</h2>
  {{ range .TaskInfoItems }}
  <b>{{ index . 0 }} : </b>{{ index . 1 }}<br>
  {{ end }}
  <h2>Workload Information:</h2>
  {{ range .WorkloadInfoItems }}
  <b>{{ index . 0 }} : </b>{{ index . 1 }}<br>
  {{ end }}
  <h2>Execution Information:</h2>
  {{ range .ExecutionInfoItems }}
  <b>{{ index . 0 }} : </b>{{ index . 1 }}<br>
  {{ end }}
  <h2>Report Summary:</h2>
  <table>
   <tr>
    <th>SQL Categiry</th>
    <th>SQL Count</th>
    <th>Plan Change Count</th>
   </tr>
   <tr>
    <td>Overall</td>
    <td>{{ .Summary.Overall.SQL }}</td>
    <td>{{ .Summary.Overall.Plan }}</td>
   </tr>
   <tr>
    <td>Improved</td>
    <td>{{ .Summary.Improved.SQL }}</td>
    <td>{{ .Summary.Improved.Plan }}</td>
   </tr>
   <tr>
    <td>Unchanged</td>
    <td>{{ .Summary.Unchanged.SQL }}</td>
    <td>{{ .Summary.Unchanged.Plan }}</td>
   </tr>
   <tr>
    <td>May Degraded</td>
    <td>{{ .Summary.MayDegraded.SQL }}</td>
    <td>{{ .Summary.MayDegraded.Plan }}</td>
   </tr>
   <tr>
    <td>With Errors</td>
    <td>{{ .Summary.Errors.SQL }}</td>
    <td>{{ .Summary.Errors.Plan }}</td>
   </tr>
   <tr>
    <td>Unsupported</td>
    <td>{{ .Summary.Unsupported.SQL }}</td>
    <td>{{ .Summary.Unsupported.Plan }}</td>
   </tr>
  </table> 
 </body>
</html>`