using System;
using System.Collections.Generic;
using System.Reflection;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Util;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
{
	/// <summary>
	/// Factory class for registering and searching for expressions for use in the
	/// <see cref="Find"/>
	/// command.
	/// </summary>
	internal sealed class ExpressionFactory
	{
		private const string RegisterExpressionMethod = "registerExpression";

		private IDictionary<string, Type> expressionMap = new Dictionary<string, Type>();

		private static readonly Org.Apache.Hadoop.FS.Shell.Find.ExpressionFactory Instance
			 = new Org.Apache.Hadoop.FS.Shell.Find.ExpressionFactory();

		internal static Org.Apache.Hadoop.FS.Shell.Find.ExpressionFactory GetExpressionFactory
			()
		{
			return Instance;
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
		internal void RegisterExpression(Type expressionClass)
		{
			try
			{
				MethodInfo register = expressionClass.GetMethod(RegisterExpressionMethod, typeof(
					Org.Apache.Hadoop.FS.Shell.Find.ExpressionFactory));
				if (register != null)
				{
					register.Invoke(null, this);
				}
			}
			catch (Exception e)
			{
				throw new RuntimeException(StringUtils.StringifyException(e));
			}
		}

		/// <summary>Register the given class as handling the given list of expression names.
		/// 	</summary>
		/// <param name="expressionClass">the class implementing the expression names</param>
		/// <param name="names">one or more command names that will invoke this class</param>
		/// <exception cref="System.IO.IOException">if the expression is not of an expected type
		/// 	</exception>
		internal void AddClass(Type expressionClass, params string[] names)
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
		internal bool IsExpression(string expressionName)
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
		internal Expression GetExpression(string expressionName, Configuration conf)
		{
			if (conf == null)
			{
				throw new ArgumentNullException("configuration is null");
			}
			Type expressionClass = expressionMap[expressionName];
			Expression instance = CreateExpression(expressionClass, conf);
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
		internal Expression CreateExpression(Type expressionClass, Configuration conf)
		{
			Expression instance = null;
			if (expressionClass != null)
			{
				instance = ReflectionUtils.NewInstance(expressionClass, conf);
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
		internal Expression CreateExpression(string expressionClassname, Configuration conf
			)
		{
			try
			{
				Type expressionClass = Sharpen.Runtime.GetType(expressionClassname).AsSubclass<Expression
					>();
				return CreateExpression(expressionClass, conf);
			}
			catch (TypeLoadException)
			{
				throw new ArgumentException("Invalid classname " + expressionClassname);
			}
		}
	}
}
