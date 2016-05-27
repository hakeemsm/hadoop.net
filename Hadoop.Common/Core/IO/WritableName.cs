using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// Utility to permit renaming of Writable implementation classes without
	/// invalidiating files that contain their class name.
	/// </summary>
	public class WritableName
	{
		private static Dictionary<string, Type> NameToClass = new Dictionary<string, Type
			>();

		private static Dictionary<Type, string> ClassToName = new Dictionary<Type, string
			>();

		static WritableName()
		{
			// define important types
			Org.Apache.Hadoop.IO.WritableName.SetName(typeof(NullWritable), "null");
			Org.Apache.Hadoop.IO.WritableName.SetName(typeof(LongWritable), "long");
			Org.Apache.Hadoop.IO.WritableName.SetName(typeof(UTF8), "UTF8");
			Org.Apache.Hadoop.IO.WritableName.SetName(typeof(MD5Hash), "MD5Hash");
		}

		private WritableName()
		{
		}

		// no public ctor
		/// <summary>
		/// Set the name that a class should be known as to something other than the
		/// class name.
		/// </summary>
		public static void SetName(Type writableClass, string name)
		{
			lock (typeof(WritableName))
			{
				ClassToName[writableClass] = name;
				NameToClass[name] = writableClass;
			}
		}

		/// <summary>Add an alternate name for a class.</summary>
		public static void AddName(Type writableClass, string name)
		{
			lock (typeof(WritableName))
			{
				NameToClass[name] = writableClass;
			}
		}

		/// <summary>Return the name for a class.</summary>
		/// <remarks>
		/// Return the name for a class.  Default is
		/// <see cref="System.Type{T}.FullName()"/>
		/// .
		/// </remarks>
		public static string GetName(Type writableClass)
		{
			lock (typeof(WritableName))
			{
				string name = ClassToName[writableClass];
				if (name != null)
				{
					return name;
				}
				return writableClass.FullName;
			}
		}

		/// <summary>Return the class for a name.</summary>
		/// <remarks>
		/// Return the class for a name.  Default is
		/// <see cref="Sharpen.Runtime.GetType(string)"/>
		/// .
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static Type GetClass(string name, Configuration conf)
		{
			lock (typeof(WritableName))
			{
				Type writableClass = NameToClass[name];
				if (writableClass != null)
				{
					return writableClass.AsSubclass<Writable>();
				}
				try
				{
					return conf.GetClassByName(name);
				}
				catch (TypeLoadException e)
				{
					IOException newE = new IOException("WritableName can't load class: " + name);
					Sharpen.Extensions.InitCause(newE, e);
					throw newE;
				}
			}
		}
	}
}
