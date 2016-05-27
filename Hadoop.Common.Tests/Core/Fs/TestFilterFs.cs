using System;
using System.Collections.Generic;
using System.Reflection;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.FS
{
	public class TestFilterFs : TestCase
	{
		private static readonly Log Log = FileSystem.Log;

		public class DontCheck
		{
			public virtual void CheckScheme(URI uri, string supportedScheme)
			{
			}

			public virtual IEnumerator<FileStatus> ListStatusIterator(Path f)
			{
				return null;
			}

			public virtual IEnumerator<LocatedFileStatus> ListLocatedStatus(Path f)
			{
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFilterFileSystem()
		{
			foreach (MethodInfo m in Sharpen.Runtime.GetDeclaredMethods(typeof(AbstractFileSystem
				)))
			{
				if (Modifier.IsStatic(m.GetModifiers()))
				{
					continue;
				}
				if (Modifier.IsPrivate(m.GetModifiers()))
				{
					continue;
				}
				if (Modifier.IsFinal(m.GetModifiers()))
				{
					continue;
				}
				try
				{
					typeof(TestFilterFs.DontCheck).GetMethod(m.Name, Sharpen.Runtime.GetParameterTypes
						(m));
					Log.Info("Skipping " + m);
				}
				catch (MissingMethodException)
				{
					Log.Info("Testing " + m);
					try
					{
						Sharpen.Runtime.GetDeclaredMethod(typeof(FilterFs), m.Name, Sharpen.Runtime.GetParameterTypes
							(m));
					}
					catch (MissingMethodException exc2)
					{
						Log.Error("FilterFileSystem doesn't implement " + m);
						throw;
					}
				}
			}
		}
	}
}
