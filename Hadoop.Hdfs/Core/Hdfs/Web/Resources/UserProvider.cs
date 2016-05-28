using System.IO;
using System.Security;
using Com.Sun.Jersey.Api.Core;
using Com.Sun.Jersey.Core.Spi.Component;
using Com.Sun.Jersey.Server.Impl.Inject;
using Com.Sun.Jersey.Spi.Inject;
using Javax.Servlet;
using Javax.Servlet.Http;
using Javax.WS.RS.Core;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Security;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Inject user information to http operations.</summary>
	public class UserProvider : AbstractHttpContextInjectable<UserGroupInformation>, 
		InjectableProvider<Context, Type>
	{
		[Context]
		internal HttpServletRequest request;

		[Context]
		internal ServletContext servletcontext;

		public override UserGroupInformation GetValue(HttpContext context)
		{
			Configuration conf = (Configuration)servletcontext.GetAttribute(JspHelper.CurrentConf
				);
			try
			{
				return JspHelper.GetUGI(servletcontext, request, conf, UserGroupInformation.AuthenticationMethod
					.Kerberos, false);
			}
			catch (IOException e)
			{
				throw new SecurityException(SecurityUtil.FailedToGetUgiMsgHeader + " " + e, e);
			}
		}

		public virtual ComponentScope GetScope()
		{
			return ComponentScope.PerRequest;
		}

		public virtual Injectable<UserGroupInformation> GetInjectable(ComponentContext componentContext
			, Context context, Type type)
		{
			return type.Equals(typeof(UserGroupInformation)) ? this : null;
		}
	}
}
