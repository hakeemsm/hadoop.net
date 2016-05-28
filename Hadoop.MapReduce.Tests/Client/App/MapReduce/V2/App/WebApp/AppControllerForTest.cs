using System;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	/// <summary>Class AppControllerForTest overrides some methods of AppController for test
	/// 	</summary>
	public class AppControllerForTest : AppController
	{
		private static readonly IDictionary<string, string> properties = new Dictionary<string
			, string>();

		private ResponseInfo responseInfo = new ResponseInfo();

		private View view = new ViewForTest();

		private Type clazz;

		private HttpServletResponse response;

		protected internal AppControllerForTest(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App
			 app, Configuration configuration, Controller.RequestContext ctx)
			: base(app, configuration, ctx)
		{
			writer = new PrintWriter(data);
		}

		public virtual Type GetClazz()
		{
			return clazz;
		}

		public override T GetInstance<T>()
		{
			System.Type cls = typeof(T);
			clazz = cls;
			if (cls.Equals(typeof(ResponseInfo)))
			{
				return (T)responseInfo;
			}
			return (T)view;
		}

		public virtual ResponseInfo GetResponseInfo()
		{
			return responseInfo;
		}

		public override string Get(string key, string defaultValue)
		{
			string result = properties[key];
			if (result == null)
			{
				result = defaultValue;
			}
			return result;
		}

		public override void Set(string key, string value)
		{
			properties[key] = value;
		}

		public override HttpServletRequest Request()
		{
			HttpServletRequest result = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(result.GetRemoteUser()).ThenReturn("user");
			return result;
		}

		public override HttpServletResponse Response()
		{
			if (response == null)
			{
				response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			}
			return response;
		}

		public virtual IDictionary<string, string> GetProperty()
		{
			return properties;
		}

		internal OutputStream data = new ByteArrayOutputStream();

		internal PrintWriter writer;

		public virtual string GetData()
		{
			writer.Flush();
			return data.ToString();
		}

		protected override PrintWriter Writer()
		{
			if (writer == null)
			{
				writer = new PrintWriter(data);
			}
			return writer;
		}
	}
}
