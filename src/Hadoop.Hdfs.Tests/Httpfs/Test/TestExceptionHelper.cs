using System;
using NUnit.Framework;
using NUnit.Framework.Rules;
using NUnit.Framework.Runners.Model;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	public class TestExceptionHelper : MethodRule
	{
		[NUnit.Framework.Test]
		public virtual void Dummy()
		{
		}

		public virtual Statement Apply(Statement statement, FrameworkMethod frameworkMethod
			, object o)
		{
			return new _Statement_37(frameworkMethod, statement);
		}

		private sealed class _Statement_37 : Statement
		{
			public _Statement_37(FrameworkMethod frameworkMethod, Statement statement)
			{
				this.frameworkMethod = frameworkMethod;
				this.statement = statement;
			}

			/// <exception cref="System.Exception"/>
			public override void Evaluate()
			{
				TestException testExceptionAnnotation = frameworkMethod.GetAnnotation<TestException
					>();
				try
				{
					statement.Evaluate();
					if (testExceptionAnnotation != null)
					{
						Type klass = testExceptionAnnotation.Exception();
						NUnit.Framework.Assert.Fail("Expected Exception: " + klass.Name);
					}
				}
				catch (Exception ex)
				{
					if (testExceptionAnnotation != null)
					{
						Type klass = testExceptionAnnotation.Exception();
						if (klass.IsInstanceOfType(ex))
						{
							string regExp = testExceptionAnnotation.MsgRegExp();
							Sharpen.Pattern pattern = Sharpen.Pattern.Compile(regExp);
							if (!pattern.Matcher(ex.Message).Find())
							{
								NUnit.Framework.Assert.Fail("Expected Exception Message pattern: " + regExp + " got message: "
									 + ex.Message);
							}
						}
						else
						{
							NUnit.Framework.Assert.Fail("Expected Exception: " + klass.Name + " got: " + ex.GetType
								().Name);
						}
					}
					else
					{
						throw;
					}
				}
			}

			private readonly FrameworkMethod frameworkMethod;

			private readonly Statement statement;
		}
	}
}
