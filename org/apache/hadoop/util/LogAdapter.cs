using Sharpen;

namespace org.apache.hadoop.util
{
	internal class LogAdapter
	{
		private org.apache.commons.logging.Log LOG;

		private org.slf4j.Logger LOGGER;

		private LogAdapter(org.apache.commons.logging.Log LOG)
		{
			this.LOG = LOG;
		}

		private LogAdapter(org.slf4j.Logger LOGGER)
		{
			this.LOGGER = LOGGER;
		}

		public static org.apache.hadoop.util.LogAdapter create(org.apache.commons.logging.Log
			 LOG)
		{
			return new org.apache.hadoop.util.LogAdapter(LOG);
		}

		public static org.apache.hadoop.util.LogAdapter create(org.slf4j.Logger LOGGER)
		{
			return new org.apache.hadoop.util.LogAdapter(LOGGER);
		}

		public virtual void info(string msg)
		{
			if (LOG != null)
			{
				LOG.info(msg);
			}
			else
			{
				if (LOGGER != null)
				{
					LOGGER.info(msg);
				}
			}
		}

		public virtual void warn(string msg, System.Exception t)
		{
			if (LOG != null)
			{
				LOG.warn(msg, t);
			}
			else
			{
				if (LOGGER != null)
				{
					LOGGER.warn(msg, t);
				}
			}
		}

		public virtual void debug(System.Exception t)
		{
			if (LOG != null)
			{
				LOG.debug(t);
			}
			else
			{
				if (LOGGER != null)
				{
					LOGGER.debug(string.Empty, t);
				}
			}
		}

		public virtual void error(string msg)
		{
			if (LOG != null)
			{
				LOG.error(msg);
			}
			else
			{
				if (LOGGER != null)
				{
					LOGGER.error(msg);
				}
			}
		}
	}
}
