using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class SchedulerPageUtil
	{
		internal class QueueBlockUtil : HtmlBlock
		{
			private void ReopenQueue(HtmlBlock.Block html)
			{
				html.Script().$type("text/javascript").("function reopenQueryNodes() {", "  var currentParam = window.location.href.split('?');"
					, "  var tmpCurrentParam = currentParam;", "  var queryQueuesString = '';", "  if (tmpCurrentParam.length > 1) {"
					, "    // openQueues=q1#q2&param1=value1&param2=value2", "    tmpCurrentParam = tmpCurrentParam[1];"
					, "    if (tmpCurrentParam.indexOf('openQueues=') != -1 ) {", "      tmpCurrentParam = tmpCurrentParam.split('openQueues=')[1].split('&')[0];"
					, "      queryQueuesString = tmpCurrentParam;", "    }", "  }", "  if (queryQueuesString != '') {"
					, "    queueArray = queryQueuesString.split('#');", "    $('#cs .q').each(function() {"
					, "      var name = $(this).html();", "      if (name != 'root' && $.inArray(name, queueArray) != -1) {"
					, "        $(this).closest('li').removeClass('jstree-closed').addClass('jstree-open'); "
					, "      }", "    });", "  }", "  $('#cs').bind( {", "                  'open_node.jstree' :function(e, data) { storeExpandedQueue(e, data); },"
					, "                  'close_node.jstree':function(e, data) { storeExpandedQueue(e, data); }"
					, "  });", "}").();
			}

			private void StoreExpandedQueue(HtmlBlock.Block html)
			{
				html.Script().$type("text/javascript").("function storeExpandedQueue(e, data) {", 
					"  var OPEN_QUEUES = 'openQueues';", "  var ACTION_OPEN = 'open';", "  var ACTION_CLOSED = 'closed';"
					, "  var $li = $(data.args[0]);", "  var action = ACTION_CLOSED;  //closed or open"
					, "  var queueName = ''", "  if ($li.hasClass('jstree-open')) {", "      action=ACTION_OPEN;"
					, "  }", "  queueName = $li.find('.q').html();", "  // http://localhost:8088/cluster/scheduler?openQueues=q1#q2&param1=value1&param2=value2 "
					, "  //   ==> [http://localhost:8088/cluster/scheduler , openQueues=q1#q2&param1=value1&param2=value2]"
					, "  var currentParam = window.location.href.split('?');", "  var tmpCurrentParam = currentParam;"
					, "  var queryString = '';", "  if (tmpCurrentParam.length > 1) {", "    // openQueues=q1#q2&param1=value1&param2=value2"
					, "    tmpCurrentParam = tmpCurrentParam[1];", "    currentParam = tmpCurrentParam;"
					, "    tmpCurrentParam = tmpCurrentParam.split('&');", "    var len = tmpCurrentParam.length;"
					, "    var paramExist = false;", "    if (len > 1) {    // Currently no query param are present but in future if any are added for that handling it now"
					, "      queryString = '';", "      for (var i = 0 ; i < len ; i++) {  // searching for param openQueues"
					, "        if (tmpCurrentParam[i].substr(0,11) == OPEN_QUEUES + '=') {", "          if (action == ACTION_OPEN) {"
					, "            tmpCurrentParam[i] = addQueueName(tmpCurrentParam[i],queueName);"
					, "          }", "          else if (action == ACTION_CLOSED) {", "            tmpCurrentParam[i] = removeQueueName(tmpCurrentParam[i] , queueName);"
					, "          }", "          paramExist = true;", "        }", "        if (i > 0) {"
					, "          queryString += '&';", "        }", "        queryString += tmpCurrentParam[i];"
					, "      }", "      // If in existing query string OPEN_QUEUES param is not present"
					, "      if (action == ACTION_OPEN && !paramExist) {", "        queryString = currentParam + '&' + OPEN_QUEUES + '=' + queueName;"
					, "      }", "    } ", "    // Only one param is present in current query string"
					, "    else {", "      tmpCurrentParam=tmpCurrentParam[0];", "      // checking if the only param present in query string is OPEN_QUEUES or not and making queryString accordingly"
					, "      if (tmpCurrentParam.substr(0,11) == OPEN_QUEUES + '=') {", "        if (action == ACTION_OPEN) {"
					, "          queryString = addQueueName(tmpCurrentParam,queueName);", "        }"
					, "        else if (action == ACTION_CLOSED) {", "          queryString = removeQueueName(tmpCurrentParam , queueName);"
					, "        }", "      }", "      else {", "        if (action == ACTION_OPEN) {"
					, "          queryString = tmpCurrentParam + '&' + OPEN_QUEUES + '=' + queueName;"
					, "        }", "      }", "    }", "  } else {", "    if (action == ACTION_OPEN) {"
					, "      tmpCurrentParam = '';", "      currentParam = tmpCurrentParam;", "      queryString = OPEN_QUEUES+'='+queueName;"
					, "    }", "  }", "  if (queryString != '') {", "    queryString = '?' + queryString;"
					, "  }", "  var url = window.location.protocol + '//' + window.location.host + window.location.pathname + queryString;"
					, "  window.history.pushState( { path : url }, '', url);", "};", string.Empty, "function removeQueueName(queryString, queueName) {"
					, "  var index = queryString.indexOf(queueName);", "  // Finding if queue is present in query param then only remove it"
					, "  if (index != -1) {", "    // removing openQueues=", "    var tmp = queryString.substr(11, queryString.length);"
					, "    tmp = tmp.split('#');", "    var len = tmp.length;", "    var newQueryString = '';"
					, "    for (var i = 0 ; i < len ; i++) {", "      if (tmp[i] != queueName) {", "        if (newQueryString != '') {"
					, "          newQueryString += '#';", "        }", "        newQueryString += tmp[i];"
					, "      }", "    }", "    queryString = newQueryString;", "    if (newQueryString != '') {"
					, "      queryString = 'openQueues=' + newQueryString;", "    }", "  }", "  return queryString;"
					, "}", string.Empty, "function addQueueName(queryString, queueName) {", "  queueArray = queryString.split('#');"
					, "  if ($.inArray(queueArray, queueName) == -1) {", "    queryString = queryString + '#' + queueName;"
					, "  }", "  return queryString;", "}").();
			}

			protected override void Render(HtmlBlock.Block html)
			{
				ReopenQueue(html);
				StoreExpandedQueue(html);
			}
		}
	}
}
