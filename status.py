class Status:
  def __init__(self, logPath=None):
    self.logFile = open(logPath, "w")
    self.stats = {}

  # Get list of all stats being checked
  def getStatNames(self):
    return self.stats.keys()

  # Return a specific stat
  def getStat(self, statName):
    return self.stats.get(statName)

  # Increment a counter with a given name (initialize if necessary)
  def count(self, counterName):
    counter = self.stats.get(counterName)
    
    if counter:
      self.updateStats({counterName: counter + 1})
    else:
      self.updateStats({counterName: 1})
      
  # Take an object, merge it into the current stats object, and print the updated stats
  def updateStats(self, stats):
    # Merge in the stats object
    for key, val in stats.items():
      self.stats[key] = val

    statsStrs = sorted([str(key) + ": " + str(val) for key, val in self.stats.items()])
    statStr = " | ".join(statsStrs)
    
    # Write the status to a logfile
    self.logFile.write(statStr + "\n")

    # Print the status
    print("\r" + statStr, end="", flush=True)

  # Give a new line at the end of reporting status
  def end(self):
    self.logFile.close()
    print("\nEnding status\n") 
