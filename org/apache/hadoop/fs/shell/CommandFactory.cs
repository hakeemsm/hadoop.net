using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>class to search for and register commands</summary>
	public class CommandFactory : org.apache.hadoop.conf.Configured
	{
		private System.Collections.Generic.IDictionary<string, java.lang.Class> classMap = 
			new System.Collections.Generic.Dictionary<string, java.lang.Class>();

		private System.Collections.Generic.IDictionary<string, org.apache.hadoop.fs.shell.Command
			> objectMap = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.fs.shell.Command
			>();

		/// <summary>Factory constructor for commands</summary>
		public CommandFactory()
			: this(null)
		{
		}

		/// <summary>Factory constructor for commands</summary>
		/// <param name="conf">the hadoop configuration</param>
		public CommandFactory(org.apache.hadoop.conf.Configuration conf)
			: base(conf)
		{
		}

		/// <summary>Invokes "static void registerCommands(CommandFactory)" on the given class.
		/// 	</summary>
		/// <remarks>
		/// Invokes "static void registerCommands(CommandFactory)" on the given class.
		/// This method abstracts the contract between the factory and the command
		/// class.  Do not assume that directly invoking registerCommands on the
		/// given class will have the same effect.
		/// </remarks>
		/// <param name="registrarClass">class to allow an opportunity to register</param>
		public virtual void registerCommands(java.lang.Class registrarClass)
		{
			try
			{
				registrarClass.getMethod("registerCommands", Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.fs.shell.CommandFactory))).invoke(null, this);
			}
			catch (System.Exception e)
			{
				throw new System.Exception(org.apache.hadoop.util.StringUtils.stringifyException(
					e));
			}
		}

		/// <summary>
		/// Register the given class as handling the given list of command
		/// names.
		/// </summary>
		/// <param name="cmdClass">the class implementing the command names</param>
		/// <param name="names">one or more command names that will invoke this class</param>
		public virtual void addClass(java.lang.Class cmdClass, params string[] names)
		{
			foreach (string name in names)
			{
				classMap[name] = cmdClass;
			}
		}

		/// <summary>
		/// Register the given object as handling the given list of command
		/// names.
		/// </summary>
		/// <remarks>
		/// Register the given object as handling the given list of command
		/// names.  Avoid calling this method and use
		/// <see cref="addClass(java.lang.Class{T}, string[])"/>
		/// whenever possible to avoid
		/// startup overhead from excessive command object instantiations.  This
		/// method is intended only for handling nested non-static classes that
		/// are re-usable.  Namely -help/-usage.
		/// </remarks>
		/// <param name="cmdObject">the object implementing the command names</param>
		/// <param name="names">one or more command names that will invoke this class</param>
		public virtual void addObject(org.apache.hadoop.fs.shell.Command cmdObject, params 
			string[] names)
		{
			foreach (string name in names)
			{
				objectMap[name] = cmdObject;
				classMap[name] = null;
			}
		}

		// just so it shows up in the list of commands
		/// <summary>Returns an instance of the class implementing the given command.</summary>
		/// <remarks>
		/// Returns an instance of the class implementing the given command.  The
		/// class must have been registered via
		/// <see cref="addClass(java.lang.Class{T}, string[])"/>
		/// </remarks>
		/// <param name="cmd">name of the command</param>
		/// <returns>instance of the requested command</returns>
		public virtual org.apache.hadoop.fs.shell.Command getInstance(string cmd)
		{
			return getInstance(cmd, getConf());
		}

		/// <summary>Get an instance of the requested command</summary>
		/// <param name="cmdName">name of the command to lookup</param>
		/// <param name="conf">the hadoop configuration</param>
		/// <returns>
		/// the
		/// <see cref="Command"/>
		/// or null if the command is unknown
		/// </returns>
		public virtual org.apache.hadoop.fs.shell.Command getInstance(string cmdName, org.apache.hadoop.conf.Configuration
			 conf)
		{
			if (conf == null)
			{
				throw new System.ArgumentNullException("configuration is null");
			}
			org.apache.hadoop.fs.shell.Command instance = objectMap[cmdName];
			if (instance == null)
			{
				java.lang.Class cmdClass = classMap[cmdName];
				if (cmdClass != null)
				{
					instance = org.apache.hadoop.util.ReflectionUtils.newInstance(cmdClass, conf);
					instance.setName(cmdName);
					instance.setCommandFactory(this);
				}
			}
			return instance;
		}

		/// <summary>Gets all of the registered commands</summary>
		/// <returns>a sorted list of command names</returns>
		public virtual string[] getNames()
		{
			string[] names = Sharpen.Collections.ToArray(classMap.Keys, new string[0]);
			java.util.Arrays.sort(names);
			return names;
		}
	}
}
