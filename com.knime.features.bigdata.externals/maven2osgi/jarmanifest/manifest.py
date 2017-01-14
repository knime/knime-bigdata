"""
Module to handle jar manifest files
"""
from jarmanifest import log, archives

logger = log.getLogger('util.manifest')

# From: http://docs.oracle.com/javase/1.4.2/docs/guide/jar/jar.html#Notes on Manifest and Signature Files
# No line may be longer than 72 bytes (not characters), in its
# UTF8-encoded form. If a value would make the initial line longer
# than this, it should be continued on extra lines
# (each starting with a single SPACE).
def getAttributes(manifestFile):
	logger.debug('Parsing attributes from %s'%(manifestFile))

	mf = open(manifestFile,'r',encoding='utf8')
	manifest = []
	current = {}
	lines = mf.readlines()
	idx = 0
	while idx<len(lines):
		line = lines[idx]
		if len(line) > 0:
			if line[-1] == '\n':
				line = line[0:-1]
			splits = line.split(':')
			key = splits[0].strip()
			if len(key) == 0:
				break
				
			value = ':'.join(splits[1:]).lstrip()
			current[key] = value
			while len(line.encode('utf-8')) >= 70:
				if idx<len(lines)-1:
					next_line = lines[idx+1]
					if next_line[-1] == '\n':
						next_line = next_line[0:-1]

					if (len(next_line) > 0) and (next_line[0] == ' '):
						current[key] += next_line.lstrip()
						idx += 1
						line = next_line
						continue
				break
		elif len(current)>0 :
			count = len(current.values())
			# We need to weed out name only entries
			# We can manually interrogate packages
			if count > 1 or (count==1 and 'name' not in current.keys()):
				manifest.append(current)
			current = {}
		idx += 1
	if current != {}:
		manifest.append(current)
		
	ret = {}
	for m in manifest:
		ret.update(m)
	return ret
