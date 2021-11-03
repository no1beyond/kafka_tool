package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
)

func main() {
	var (
		hosts string
	)
	flag.StringVar(&hosts, "hosts", "", "kafka broker hosts")
	flag.Parse()

	if len(hosts) == 0 {
		flag.PrintDefaults()
		return
	}

	adminConfig := sarama.NewConfig()
	adminConfig.Version = sarama.V1_1_1_0
	admin, err := sarama.NewClusterAdmin(strings.Split(hosts, ","), adminConfig)
	if err != nil {
		fmt.Println("sarama.NewClusterAdmin", hosts, err)
		return
	}
	cgroups, err := admin.ListConsumerGroups()
	if err != nil {
		fmt.Println("admin.ListConsumerGroups", hosts, err)
		return
	}
	cglist := make([]string, 0, len(cgroups))
	for k, v := range cgroups {
		fmt.Printf("cgroups,k:%s, v:%s\n", k, v)
		cglist = append(cglist, k)
	}

	desc, err := admin.DescribeConsumerGroups(cglist)
	if err != nil {
		fmt.Println("admin.DescribeConsumerGroups", cglist, err)
		return
	}
	for _, v := range desc {
		for id, m := range v.Members {
			assignment, _ := m.GetMemberAssignment()
			for t, v1 := range assignment.Topics {
				fmt.Printf("%s,%s,%s,%s,%+v\n", v.GroupId, id, m.ClientHost, t, v1)
			}
		}

	}
}
