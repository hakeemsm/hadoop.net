using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Common
{
	public sealed class Util
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Util).FullName);

		/// <summary>Interprets the passed string as a URI.</summary>
		/// <remarks>
		/// Interprets the passed string as a URI. In case of error it
		/// assumes the specified string is a file.
		/// </remarks>
		/// <param name="s">the string to interpret</param>
		/// <returns>the resulting URI</returns>
		/// <exception cref="System.IO.IOException"></exception>
		public static URI StringAsURI(string s)
		{
			URI u = null;
			// try to make a URI
			try
			{
				u = new URI(s);
			}
			catch (URISyntaxException e)
			{
				Log.Error("Syntax error in URI " + s + ". Please check hdfs configuration.", e);
			}
			// if URI is null or scheme is undefined, then assume it's file://
			if (u == null || u.GetScheme() == null)
			{
				Log.Warn("Path " + s + " should be specified as a URI " + "in configuration files. Please update hdfs configuration."
					);
				u = FileAsURI(new FilePath(s));
			}
			return u;
		}

		/// <summary>Converts the passed File to a URI.</summary>
		/// <remarks>
		/// Converts the passed File to a URI. This method trims the trailing slash if
		/// one is appended because the underlying file is in fact a directory that
		/// exists.
		/// </remarks>
		/// <param name="f">the file to convert</param>
		/// <returns>the resulting URI</returns>
		/// <exception cref="System.IO.IOException"/>
		public static URI FileAsURI(FilePath f)
		{
			URI u = f.GetCanonicalFile().ToURI();
			// trim the trailing slash, if it's present
			if (u.GetPath().EndsWith("/"))
			{
				string uriAsString = u.ToString();
				try
				{
					u = new URI(Sharpen.Runtime.Substring(uriAsString, 0, uriAsString.Length - 1));
				}
				catch (URISyntaxException e)
				{
					throw new IOException(e);
				}
			}
			return u;
		}

		/// <summary>Converts a collection of strings into a collection of URIs.</summary>
		/// <param name="names">collection of strings to convert to URIs</param>
		/// <returns>collection of URIs</returns>
		public static IList<URI> StringCollectionAsURIs(ICollection<string> names)
		{
			IList<URI> uris = new AList<URI>(names.Count);
			foreach (string name in names)
			{
				try
				{
					uris.AddItem(StringAsURI(name));
				}
				catch (IOException e)
				{
					Log.Error("Error while processing URI: " + name, e);
				}
			}
			return uris;
		}
	}
}
