package postgres

type ConsumerHealth struct {
	isHealthy      bool
	unhealthyCount int
}

func (c *ConsumerHealth) SetHealth(health bool) {
	if health {
		c.isHealthy = true
		c.unhealthyCount = 0
	} else if !health && c.unhealthyCount < 3 {
		c.unhealthyCount++
	} else {
		c.isHealthy = false
	}
}

func (c *ConsumerHealth) Shutdown() {
	c.isHealthy = false
}

func (c *ConsumerHealth) GetHealth() bool {
	return c.isHealthy
}
