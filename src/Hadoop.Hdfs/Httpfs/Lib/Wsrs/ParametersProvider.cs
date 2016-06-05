using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Com.Sun.Jersey.Api.Core;
using Com.Sun.Jersey.Core.Spi.Component;
using Com.Sun.Jersey.Server.Impl.Inject;
using Com.Sun.Jersey.Spi.Inject;
using Javax.WS.RS.Core;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	/// <summary>
	/// Jersey provider that parses the request parameters based on the
	/// given parameter definition.
	/// </summary>
	public class ParametersProvider : AbstractHttpContextInjectable<Parameters>, InjectableProvider
		<Context, Type>
	{
		private string driverParam;

		private Type enumClass;

		private IDictionary<Enum, Type[]> paramsDef;

		public ParametersProvider(string driverParam, Type enumClass, IDictionary<Enum, Type
			[]> paramsDef)
		{
			this.driverParam = driverParam;
			this.enumClass = enumClass;
			this.paramsDef = paramsDef;
		}

		public override Parameters GetValue(HttpContext httpContext)
		{
			IDictionary<string, IList<Param<object>>> map = new Dictionary<string, IList<Param
				<object>>>();
			IDictionary<string, IList<string>> queryString = httpContext.GetRequest().GetQueryParameters
				();
			string str = ((MultivaluedMap<string, string>)queryString).GetFirst(driverParam);
			if (str == null)
			{
				throw new ArgumentException(MessageFormat.Format("Missing Operation parameter [{0}]"
					, driverParam));
			}
			Enum op;
			try
			{
				op = Enum.ValueOf(enumClass, StringUtils.ToUpperCase(str));
			}
			catch (ArgumentException)
			{
				throw new ArgumentException(MessageFormat.Format("Invalid Operation [{0}]", str));
			}
			if (!paramsDef.Contains(op))
			{
				throw new ArgumentException(MessageFormat.Format("Unsupported Operation [{0}]", op
					));
			}
			foreach (Type paramClass in paramsDef[op])
			{
				Param<object> param = NewParam(paramClass);
				IList<Param<object>> paramList = Lists.NewArrayList();
				IList<string> ps = queryString[param.GetName()];
				if (ps != null)
				{
					foreach (string p in ps)
					{
						try
						{
							param.ParseParam(p);
						}
						catch (Exception ex)
						{
							throw new ArgumentException(ex.ToString(), ex);
						}
						paramList.AddItem(param);
						param = NewParam(paramClass);
					}
				}
				else
				{
					paramList.AddItem(param);
				}
				map[param.GetName()] = paramList;
			}
			return new Parameters(map);
		}

		private Param<object> NewParam(Type paramClass)
		{
			try
			{
				return System.Activator.CreateInstance(paramClass);
			}
			catch (Exception)
			{
				throw new NotSupportedException(MessageFormat.Format("Param class [{0}] does not have default constructor"
					, paramClass.FullName));
			}
		}

		public virtual ComponentScope GetScope()
		{
			return ComponentScope.PerRequest;
		}

		public virtual Injectable GetInjectable(ComponentContext componentContext, Context
			 context, Type type)
		{
			return (type.Equals(typeof(Parameters))) ? this : null;
		}
	}
}
