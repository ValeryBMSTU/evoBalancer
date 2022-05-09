package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"time"
)

var predicts map[string]map[string]float64

type Cluster struct {
	Nodes []Node `json:"nodes"`
}

type Node struct {
	ID    string `json:"id"`
	Mem   int    `json:"mem"`
	Cores int    `json:"cores"`
	//Applications []*App `json:"Applications"`
	//taskPool []*task
	//corePool []*core
	Tasks []*Task
	//CorePool []*Core
	//appPointer int
}

func (n *Node) Send(t *Task) {

	time := predicts["execute_time"]
	t.RemainingWork += time["base"]
	t.RemainingWork += time[n.ID]
	for key, v := range t.Params {
		switch value := v.(type) {
		default:
			fmt.Printf("unexpected type %T", value) // %T prints whatever type t has
		case string:
			if val, ok := time[value]; ok {
				t.RemainingWork += val
			}
		case float64:
			t.RemainingWork += t.Params[key].(float64) * time[key]
		}
	}

	n.Tasks = append(n.Tasks, t)

	//if len(n.Applications) <= 0 {
	//	fmt.Println("Количество запущенных приложений равно 0 !!!")
	//	return
	//}
	//
	//minApp := n.Applications[0]
	//for _, app := range n.Applications {
	//	if len(app.Tasks) < len(minApp.Tasks) {
	//		minApp = app
	//	}
	//}
	//minApp.Send(t)
}

func (n *Node) Work() {
	for i := 0; i < n.Cores; i++ {
		if len(n.Tasks) > i {
			n.Tasks[i].RemainingWork -= 1
		}
	}

	for i := 0; i < len(n.Tasks); i++ {
		if n.Tasks[i].RemainingWork <= 0 {
			observer.times = append(observer.times, observer.CurrentTick-n.Tasks[i].Time)
			n.Tasks = remove(n.Tasks, i)
			i -= 1
		}
	}
}

func (n *Node) Status() int {
	return len(n.Tasks)
}

//type Endpoint struct {
//	ID string `json:"id"`
//}

//type App struct {
//	ID     string `json:"id"`
//	MemMb  int    `json:"mem_mb"`
//	Tasks  []*Task
//	Cores  []*Core
//	speed  int
//	NodeID string
//}
//
//func (a *App) Send(t *Task) {
//	a.Tasks = append(a.Tasks, t)
//}
//
//func (a *App) Work() {
//	for i, _ := range a.Cores {
//		if len(a.Tasks) >= i {
//			a.Tasks[i].RemainingWork -= a.speed
//		}
//	}
//
//	for i := 0; i < len(a.Tasks); i++ {
//		if a.Tasks[i].RemainingWork <= 0 {
//			a.Tasks = remove(a.Tasks, i)
//			i -= 1
//		}
//	}
//}
//
//func (a *App) Status() int {
//	return len(a.Tasks)
//}

type Core struct {
	cacheKb int
	freqGhz float64
}

func (c *Core) work(t *Task) {
	t.RemainingWork -= c.freqGhz
}

type Task struct {
	Time   int                    `json:"time"`
	Params map[string]interface{} `json:"params"`
	//AppID         string `json:"app_id"`
	RemainingWork float64
}

type balancer interface {
	init(nodes []*Node, rrWeights map[string]float64)
	balance(t *Task)
}

//type random struct {
//	nodePool []*Node
//}
//
//func (r *random) init(nodes []*Node) {
//	r.nodePool = nodes
//}
//
//func (r *random) balance(t *Task) {
//	i := rand.Intn(len(r.nodePool))
//	r.nodePool[i].send(t)
//}

//type roundRobin struct {
//	current  int
//	nodePool []*Node
//}
//
//func (r *roundRobin) init(nodes []*Node) {
//	r.current = 0
//	r.nodePool = nodes
//}
//
//func (r *roundRobin) balance(t task) {
//	r.nodePool[r.current].send(t)
//	r.current += 1
//	if r.current == len(r.nodePool) {
//		r.current = 0
//	}
//}

type WeightedRoundRobin struct {
	NodePool []*WeightedNode
}

type WeightedNode struct {
	Node    *Node
	Weight  float64
	Current float64
}

func (r *WeightedRoundRobin) init(nodes []*Node, rrWeights map[string]float64) {
	for _, node := range nodes {
		weight := rrWeights[node.ID]

		weightedApp := &WeightedNode{
			Node:    node,
			Weight:  weight,
			Current: 0,
		}

		r.NodePool = append(r.NodePool, weightedApp)
	}
}

func (r *WeightedRoundRobin) balance(t *Task) {
	if len(r.NodePool) <= 0 {
		fmt.Println("AppPool is empty!!!")
		return
	}

	var minNode *WeightedNode
	minNode = nil

	for _, node := range r.NodePool {
		if minNode == nil || node.Current < minNode.Current {
			minNode = node
		}
	}
	minNode.Node.Send(t)
	minNode.Current += 1 / float64(minNode.Weight)
}

type Result struct {
	Ticks int              `json:"ticks"`
	Nodes map[string][]int `json:"nodes"`
}

type EvoResult struct {
	Scores []float64            `json:"scores"`
	Nodes  map[string][]float64 `json:"nodes"`
}

type config struct {
	Ages           int     `json:"ages"`
	Agents         int     `json:"agents"`
	MutationPower  float64 `json:"mutation_power"`
	MutationChance float64 `json:"mutation_chance"`
	DropPart       float64 `json:"drop_part"`
}

type Observer struct {
	CurrentTick int
	times       []int
}

type Agent struct {
	Result  Result
	Score   int
	Weights map[string]float64
}

var observer Observer

func main() {
	dataWeights := readFile("./data/weights.json")

	weights := make(map[string]float64)
	err := json.Unmarshal(dataWeights, &weights)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(weights)

	dataConfig := readFile("./data/config.json")

	config := config{}
	err = json.Unmarshal(dataConfig, &config)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(weights)

	agents := make([]Agent, config.Agents, config.Agents)
	for i, _ := range agents {
		agents[i] = Agent{Result{}, 0, weights}
	}

	evoResult := EvoResult{make([]float64, 0), make(map[string][]float64)}
	for key, _ := range weights {
		evoResult.Nodes[key] = make([]float64, 0)
	}
	evoResult.Scores = make([]float64, 0)

	for i := 0; i < config.Ages; i++ {
		for index, a := range agents {
			a.Result, a.Score = Run(a.Weights)

			fmt.Println("Age", i, "Agent", index, "Score", a.Score)

			dat, _ := json.Marshal(a.Result)
			fmt.Println(string(dat))

			err = ioutil.WriteFile("./data/results/age"+strconv.Itoa(i)+"_"+
				"agent"+strconv.Itoa(index)+".json", dat, 0644)
			if err != nil {
				return
			}
			agents[index] = a
		}

		agents = sort(agents)

		agents = drop(agents, config.DropPart)

		agents = replicate(agents, config.Agents, config.MutationChance, config.MutationPower)

		for key, v := range agents[0].Weights {
			evoResult.Nodes[key] = append(evoResult.Nodes[key], v)
		}
		evoResult.Scores = append(evoResult.Scores, float64(agents[0].Score))
	}

	dat, _ := json.Marshal(evoResult)
	fmt.Println(string(dat))

	err = ioutil.WriteFile("./data/results/evo_result.json", dat, 0644)
	if err != nil {
		return
	}

	fmt.Println("Kek")
}

func Run(weights map[string]float64) (res Result, score int) {
	observer = Observer{0, make([]int, 0)}

	rand.Seed(time.Now().UnixNano())

	dataNodes := readFile("./data/model.json")

	var nodes []*Node
	err := json.Unmarshal(dataNodes, &nodes)
	if err != nil {
		fmt.Println(err)
		return Result{}, -1.0
	}

	fmt.Println(nodes)

	dataTasks := readFile("./data/tasks.json")
	var tasks []*Task
	err = json.Unmarshal(dataTasks, &tasks)
	if err != nil {
		fmt.Println(err)
		return Result{}, -1.0
	}

	fmt.Println(tasks)

	dataPredicts := readFile("./data/coef.json")
	err = json.Unmarshal(dataPredicts, &predicts)
	if err != nil {
		fmt.Println(err)
		return Result{}, -1.0
	}

	fmt.Println(predicts)

	var balancer balancer

	balancer = &WeightedRoundRobin{}
	balancer.init(nodes, weights)

	result := modeling(200, nodes, balancer, tasks)

	dat, _ := json.Marshal(result)
	fmt.Println(string(dat))

	err = ioutil.WriteFile("./data/result.json", dat, 0644)
	if err != nil {
		return result, -1.0
	}

	return result, sum(observer.times)
}

func modeling(ticksCount int, nodes []*Node, bal balancer, tasks []*Task) (res Result) {
	resultNodes := make(map[string][]int, 0)

	events := make(map[int][]*Task)
	for _, v := range tasks {
		events[v.Time] = append(events[v.Time], v)
	}

	currTaskCount := 0
	resTicksCount := 0
	for i := 1; i <= ticksCount || currTaskCount > 0; i++ {
		resTicksCount += 1

		observer.CurrentTick = i
		if tasks, ok := events[i]; ok {
			for _, t := range tasks {
				bal.balance(t)
			}
		}

		for _, node := range nodes {
			node.Work()
		}

		currTaskCount = 0
		for _, node := range nodes {
			currTaskCount += node.Status()
			resultNodes[node.ID] = append(resultNodes[node.ID], node.Status())
		}
	}

	return Result{
		Ticks: resTicksCount,
		Nodes: resultNodes,
	}
}

func remove(slice []*Task, s int) []*Task {
	return append(slice[:s], slice[s+1:]...)
}

func readFile(fileName string) []byte {
	file, err := os.Open(fileName)

	if err != nil {
		panic(err.Error())
	}

	defer file.Close()

	reader := bufio.NewReader(file)
	buffer := bytes.NewBuffer(make([]byte, 0))

	var chunk []byte
	var eol bool
	var data []byte

	for {
		if chunk, eol, err = reader.ReadLine(); err != nil {
			break
		}
		buffer.Write(chunk)
		if !eol {
			data = append(data, buffer.Bytes()...)
			buffer.Reset()
		}
	}

	if err == io.EOF {
		err = nil
	}

	return data
}

func sum(arr []int) int {
	sum := 0
	for _, v := range arr {
		sum += v
	}
	return sum
}

func sort(agents []Agent) []Agent {
	for i := 0; i < len(agents); i++ {
		for j := 0; j < len(agents)-1; j++ {
			if agents[j].Score > agents[j+1].Score {
				agents[j], agents[j+1] = agents[j+1], agents[j]
			}
		}
	}
	return agents
}

func drop(agents []Agent, dropPart float64) []Agent {
	dropIndex := int(float64(len(agents)) * (1.0 - dropPart))
	return agents[:dropIndex]
}

func replicate(agents []Agent, targetCount int, mutChance, mutPower float64) []Agent {
	for _, a := range agents {
		newWeights := map[string]float64{}
		for key, v := range a.Weights {
			newWeights[key] = v
		}

		newAgent := Agent{Result{}, 0, newWeights}
		for key, w := range newAgent.Weights {
			r := rand.Float64()
			if r <= mutChance {
				p := ((rand.Float64() * 2.0) - 1.0) * mutPower
				newAgent.Weights[key] = w + (w * p)
			}
		}
		agents = append(agents, newAgent)
		if len(agents) >= targetCount {
			break
		}
	}
	return agents
}
