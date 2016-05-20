using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>
	/// FileBasedIPList loads a list of subnets in CIDR format and ip addresses from
	/// a file.
	/// </summary>
	/// <remarks>
	/// FileBasedIPList loads a list of subnets in CIDR format and ip addresses from
	/// a file.
	/// Given an ip address, isIn  method returns true if ip belongs to one of the
	/// subnets.
	/// Thread safe.
	/// </remarks>
	public class FileBasedIPList : org.apache.hadoop.util.IPList
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.FileBasedIPList
			)));

		private readonly string fileName;

		private readonly org.apache.hadoop.util.MachineList addressList;

		public FileBasedIPList(string fileName)
		{
			this.fileName = fileName;
			string[] lines;
			try
			{
				lines = readLines(fileName);
			}
			catch (System.IO.IOException)
			{
				lines = null;
			}
			if (lines != null)
			{
				addressList = new org.apache.hadoop.util.MachineList(new java.util.HashSet<string
					>(java.util.Arrays.asList(lines)));
			}
			else
			{
				addressList = null;
			}
		}

		public virtual org.apache.hadoop.util.FileBasedIPList reload()
		{
			return new org.apache.hadoop.util.FileBasedIPList(fileName);
		}

		public virtual bool isIn(string ipAddress)
		{
			if (ipAddress == null || addressList == null)
			{
				return false;
			}
			return addressList.includes(ipAddress);
		}

		/// <summary>Reads the lines in a file.</summary>
		/// <param name="fileName"/>
		/// <returns>
		/// lines in a String array; null if the file does not exist or if the
		/// file name is null
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		private static string[] readLines(string fileName)
		{
			try
			{
				if (fileName != null)
				{
					java.io.File file = new java.io.File(fileName);
					if (file.exists())
					{
						using (java.io.Reader fileReader = new java.io.InputStreamReader(new java.io.FileInputStream
							(file), org.apache.commons.io.Charsets.UTF_8))
						{
							using (java.io.BufferedReader bufferedReader = new java.io.BufferedReader(fileReader
								))
							{
								System.Collections.Generic.IList<string> lines = new System.Collections.Generic.List
									<string>();
								string line = null;
								while ((line = bufferedReader.readLine()) != null)
								{
									lines.add(line);
								}
								if (LOG.isDebugEnabled())
								{
									LOG.debug("Loaded IP list of size = " + lines.Count + " from file = " + fileName);
								}
								return (Sharpen.Collections.ToArray(lines, new string[lines.Count]));
							}
						}
					}
					else
					{
						LOG.debug("Missing ip list file : " + fileName);
					}
				}
			}
			catch (System.IO.IOException ioe)
			{
				LOG.error(ioe);
				throw;
			}
			return null;
		}
	}
}
