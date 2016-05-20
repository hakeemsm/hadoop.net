using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestFilterFs : NUnit.Framework.TestCase
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.hadoop.fs.FileSystem
			.LOG;

		public class DontCheck
		{
			public virtual void checkScheme(java.net.URI uri, string supportedScheme)
			{
			}

			public virtual System.Collections.Generic.IEnumerator<org.apache.hadoop.fs.FileStatus
				> listStatusIterator(org.apache.hadoop.fs.Path f)
			{
				return null;
			}

			public virtual System.Collections.Generic.IEnumerator<org.apache.hadoop.fs.LocatedFileStatus
				> listLocatedStatus(org.apache.hadoop.fs.Path f)
			{
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testFilterFileSystem()
		{
			foreach (java.lang.reflect.Method m in Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.AbstractFileSystem
				)).getDeclaredMethods())
			{
				if (java.lang.reflect.Modifier.isStatic(m.getModifiers()))
				{
					continue;
				}
				if (java.lang.reflect.Modifier.isPrivate(m.getModifiers()))
				{
					continue;
				}
				if (java.lang.reflect.Modifier.isFinal(m.getModifiers()))
				{
					continue;
				}
				try
				{
					Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestFilterFs.DontCheck
						)).getMethod(m.getName(), m.getParameterTypes());
					LOG.info("Skipping " + m);
				}
				catch (System.MissingMethodException)
				{
					LOG.info("Testing " + m);
					try
					{
						Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FilterFs)).getDeclaredMethod
							(m.getName(), m.getParameterTypes());
					}
					catch (System.MissingMethodException exc2)
					{
						LOG.error("FilterFileSystem doesn't implement " + m);
						throw;
					}
				}
			}
		}
	}
}
