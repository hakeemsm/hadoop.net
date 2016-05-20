using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	/// <summary>
	/// Factory class for registering and searching for expressions for use in the
	/// <see cref="Find"/>
	/// command.
	/// </summary>
	internal sealed class ExpressionFactory
	{
		private const string REGISTER_EXPRESSION_METHOD = "registerExpression";

		private System.Collections.Generic.IDictionary<string, java.lang.Class> expressionMap
			 = new System.Collections.Generic.Dictionary<string, java.lang.Class>();

		private static readonly org.apache.hadoop.fs.shell.find.ExpressionFactory INSTANCE
			 = new org.apache.hadoop.fs.shell.find.ExpressionFactory();

		internal static org.apache.hadoop.fs.shell.find.ExpressionFactory getExpressionFactory
			()
		{
			return INSTANCE;
		}

		/// <summary>Private constructor to ensure singleton.</summary>
		private ExpressionFactory()
		{
		}

		/// <summary>
		/// Invokes "static void registerExpression(FindExpressionFactory)" on the
		/// given class.
		/// </summary>
		/// <remarks>
		/// Invokes "static void registerExpression(FindExpressionFactory)" on the
		/// given class. This method abstracts the contract between the factory and the
		/// expression class. Do not assume that directly invoking registerExpression
		/// on the given class will have the same effect.
		/// </remarks>
		/// <param name="expressionClass">class to allow an opportunity to register</param>
		internal void registerExpression(java.lang.Class expressionClass)
		{
			try
			{
				java.lang.reflect.Method register = expressionClass.getMethod(REGISTER_EXPRESSION_METHOD
					, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.ExpressionFactory
					)));
				if (register != null)
				{
					register.invoke(null, this);
				}
			}
			catch (System.Exception e)
			{
				throw new System.Exception(org.apache.hadoop.util.StringUtils.stringifyException(
					e));
			}
		}

		/// <summary>Register the given class as handling the given list of expression names.
		/// 	</summary>
		/// <param name="expressionClass">the class implementing the expression names</param>
		/// <param name="names">one or more command names that will invoke this class</param>
		/// <exception cref="System.IO.IOException">if the expression is not of an expected type
		/// 	</exception>
		internal void addClass(java.lang.Class expressionClass, params string[] names)
		{
			foreach (string name in names)
			{
				expressionMap[name] = expressionClass;
			}
		}

		/// <summary>
		/// Determines whether the given expression name represents and actual
		/// expression.
		/// </summary>
		/// <param name="expressionName">name of the expression</param>
		/// <returns>true if expressionName represents an expression</returns>
		internal bool isExpression(string expressionName)
		{
			return expressionMap.Contains(expressionName);
		}

		/// <summary>Get an instance of the requested expression</summary>
		/// <param name="expressionName">name of the command to lookup</param>
		/// <param name="conf">the Hadoop configuration</param>
		/// <returns>
		/// the
		/// <see cref="Expression"/>
		/// or null if the expression is unknown
		/// </returns>
		internal org.apache.hadoop.fs.shell.find.Expression getExpression(string expressionName
			, org.apache.hadoop.conf.Configuration conf)
		{
			if (conf == null)
			{
				throw new System.ArgumentNullException("configuration is null");
			}
			java.lang.Class expressionClass = expressionMap[expressionName];
			org.apache.hadoop.fs.shell.find.Expression instance = createExpression(expressionClass
				, conf);
			return instance;
		}

		/// <summary>
		/// Creates an instance of the requested
		/// <see cref="Expression"/>
		/// class.
		/// </summary>
		/// <param name="expressionClass">
		/// <see cref="Expression"/>
		/// class to be instantiated
		/// </param>
		/// <param name="conf">the Hadoop configuration</param>
		/// <returns>
		/// a new instance of the requested
		/// <see cref="Expression"/>
		/// class
		/// </returns>
		internal org.apache.hadoop.fs.shell.find.Expression createExpression(java.lang.Class
			 expressionClass, org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.fs.shell.find.Expression instance = null;
			if (expressionClass != null)
			{
				instance = org.apache.hadoop.util.ReflectionUtils.newInstance(expressionClass, conf
					);
			}
			return instance;
		}

		/// <summary>
		/// Creates an instance of the requested
		/// <see cref="Expression"/>
		/// class.
		/// </summary>
		/// <param name="expressionClassname">
		/// name of the
		/// <see cref="Expression"/>
		/// class to be instantiated
		/// </param>
		/// <param name="conf">the Hadoop configuration</param>
		/// <returns>
		/// a new instance of the requested
		/// <see cref="Expression"/>
		/// class
		/// </returns>
		internal org.apache.hadoop.fs.shell.find.Expression createExpression(string expressionClassname
			, org.apache.hadoop.conf.Configuration conf)
		{
			try
			{
				java.lang.Class expressionClass = java.lang.Class.forName(expressionClassname).asSubclass
					<org.apache.hadoop.fs.shell.find.Expression>();
				return createExpression(expressionClass, conf);
			}
			catch (java.lang.ClassNotFoundException)
			{
				throw new System.ArgumentException("Invalid classname " + expressionClassname);
			}
		}
	}
}
