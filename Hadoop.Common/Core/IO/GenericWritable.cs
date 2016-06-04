using System;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Hadoop.Common.Core.Util;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A wrapper for Writable instances.</summary>
	/// <remarks>
	/// A wrapper for Writable instances.
	/// <p>
	/// When two sequence files, which have same Key type but different Value
	/// types, are mapped out to reduce, multiple Value types is not allowed.
	/// In this case, this class can help you wrap instances with different types.
	/// </p>
	/// <p>
	/// Compared with <code>ObjectWritable</code>, this class is much more effective,
	/// because <code>ObjectWritable</code> will append the class declaration as a String
	/// into the output file in every Key-Value pair.
	/// </p>
	/// <p>
	/// Generic Writable implements
	/// <see cref="Configurable"/>
	/// interface, so that it will be
	/// configured by the framework. The configuration is passed to the wrapped objects
	/// implementing
	/// <see cref="Configurable"/>
	/// interface <i>before deserialization</i>.
	/// </p>
	/// how to use it: <br />
	/// 1. Write your own class, such as GenericObject, which extends GenericWritable.<br />
	/// 2. Implements the abstract method <code>getTypes()</code>, defines
	/// the classes which will be wrapped in GenericObject in application.
	/// Attention: this classes defined in <code>getTypes()</code> method, must
	/// implement <code>Writable</code> interface.
	/// <br /><br />
	/// The code looks like this:
	/// <blockquote><pre>
	/// public class GenericObject extends GenericWritable {
	/// private static Class[] CLASSES = {
	/// ClassType1.class,
	/// ClassType2.class,
	/// ClassType3.class,
	/// };
	/// protected Class[] getTypes() {
	/// return CLASSES;
	/// }
	/// }
	/// </pre></blockquote>
	/// </remarks>
	/// <since>Nov 8, 2006</since>
	public abstract class GenericWritable : IWritable, Configurable
	{
		private const byte NotSet = unchecked((byte)(-1));

		private byte type = NotSet;

		private IWritable instance;

		private Configuration conf = null;

		/// <summary>Set the instance that is wrapped.</summary>
		/// <param name="obj"/>
		public virtual void Set(IWritable obj)
		{
			instance = obj;
			Type instanceClazz = instance.GetType();
			Type[] clazzes = GetTypes();
			for (int i = 0; i < clazzes.Length; i++)
			{
				Type clazz = clazzes[i];
				if (clazz.Equals(instanceClazz))
				{
					type = unchecked((byte)i);
					return;
				}
			}
			throw new RuntimeException("The type of instance is: " + instance.GetType() + ", which is NOT registered."
				);
		}

		/// <summary>Return the wrapped instance.</summary>
		public virtual IWritable Get()
		{
			return instance;
		}

		public override string ToString()
		{
			return "GW[" + (instance != null ? ("class=" + instance.GetType().FullName + ",value="
				 + instance.ToString()) : "(null)") + "]";
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			type = @in.ReadByte();
			Type clazz = GetTypes()[type & unchecked((int)(0xff))];
			try
			{
				instance = ReflectionUtils.NewInstance(clazz, conf);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				throw new IOException("Cannot initialize the class: " + clazz);
			}
			instance.ReadFields(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			if (type == NotSet || instance == null)
			{
				throw new IOException("The GenericWritable has NOT been set correctly. type=" + type
					 + ", instance=" + instance);
			}
			@out.WriteByte(type);
			instance.Write(@out);
		}

		/// <summary>Return all classes that may be wrapped.</summary>
		/// <remarks>
		/// Return all classes that may be wrapped.  Subclasses should implement this
		/// to return a constant array of classes.
		/// </remarks>
		protected internal abstract Type[] GetTypes();

		public virtual Configuration GetConf()
		{
			return conf;
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}
	}
}
