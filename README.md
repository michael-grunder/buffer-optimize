# buffer-optimize

---
A simple utility script that will read files in the Redis protocol and optimize ZINCRBY commands.  What is meant by optimization, is that the program will aggregate all ZINCRBY calls going to the same key and member as well as the total scores.  At present, all other commands are passed through.
---
