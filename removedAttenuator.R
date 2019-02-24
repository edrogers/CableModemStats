library(lubridate)
library(dplyr)
library(GGally)
library(ggplot2)

now                <- as.numeric(format(Sys.time(), "%s"))
attenuator.removal <- 1444318200
attenuator.replace <- 1445785487
amplifier.repair   <- 1457389021
service.call       <- 1462982978


speeds <- read.table("./speeds/speeds.csv",
                     header = TRUE,
                     sep = ",",
                     na.strings = "N/A",
                     colClasses = c("numeric",
                                    "numeric",
                                    "numeric",
                                    "numeric",
                                    "factor",
                                    "factor",
                                    "numeric"))

speeds <- speeds %>% arrange(Time)
speeds$HoursSinceUnixEpoch <- round(speeds$Time/3600,digits = 0)
speeds$TimePosix <- as.POSIXct(speeds$Time,origin = "1970-01-01",tz="America/Chicago")

modem <- read.table("./modemOutput/modemData.converted.csv",
                    header=TRUE,
                    sep=",",
                    na.strings = "n/a",
                    col.names=c("Time",
                                "Downstream Channel ID A",
                                "Downstream Channel ID B",
                                "Downstream Channel ID C",
                                "Downstream Channel ID D",
                                "Downstream Frequency A",
                                "Downstream Frequency B",
                                "Downstream Frequency C",
                                "Downstream Frequency D",
                                "Downstream Signal to Noise A",
                                "Downstream Signal to Noise B",
                                "Downstream Signal to Noise C",
                                "Downstream Signal to Noise D",
                                "Downstream Modulation A",
                                "Downstream Modulation B",
                                "Downstream Modulation C",
                                "Downstream Modulation D",
                                "Downstream Power Level A",
                                "Downstream Power Level B",
                                "Downstream Power Level C",
                                "Downstream Power Level D",
                                "Upstream Channel ID A",
                                "Upstream Frequency A",
                                "Upstream Ranging Service ID A",
                                "Upstream Symbol Rate A",
                                "Upstream Power Level A",
                                "Upstream Modulation A",
                                "Upstream Ranging Status A",
                                "Upstream Channel ID B",
                                "Upstream Frequency B",
                                "Upstream Ranging Service ID B",
                                "Upstream Symbol Rate B",
                                "Upstream Power Level B",
                                "Upstream Modulation B",
                                "Upstream Ranging Status B",
                                "Upstream Channel ID C",
                                "Upstream Frequency C",
                                "Upstream Ranging Service ID C",
                                "Upstream Symbol Rate C",
                                "Upstream Power Level C",
                                "Upstream Modulation C",
                                "Upstream Ranging Status C",
                                "Upstream Channel ID D",
                                "Upstream Frequency D",
                                "Upstream Ranging Service ID D",
                                "Upstream Symbol Rate D",
                                "Upstream Power Level D",
                                "Upstream Modulation D",
                                "Upstream Ranging Status D",
                                "Signal Stats Channel ID A",
                                "Signal Stats Channel ID B",
                                "Signal Stats Channel ID C",
                                "Signal Stats Channel ID D",
                                "Total Unerrored Codewords A",
                                "Total Unerrored Codewords B",
                                "Total Unerrored Codewords C",
                                "Total Unerrored Codewords D",
                                "Total Correctable Codewords A",
                                "Total Correctable Codewords B",
                                "Total Correctable Codewords C",
                                "Total Correctable Codewords D",
                                "Total Uncorrectable Codewords A",
                                "Total Uncorrectable Codewords B",
                                "Total Uncorrectable Codewords C",
                                "Total Uncorrectable Codewords D"),
                    colClasses = c("numeric",
                                   rep("factor",4),
                                   rep("factor",4),
                                   rep("numeric",4),
                                   rep("factor",4),
                                   rep("numeric",4),
                                   rep("factor",4),
                                   "numeric",
                                   rep("factor", 2),
                                   rep("factor",4),
                                   "numeric",
                                   rep("factor", 2),
                                   rep("factor",4),
                                   "numeric",
                                   rep("factor", 2),
                                   rep("factor",4),
                                   "numeric",
                                   rep("factor", 2),
                                   rep("factor",4),
                                   rep("numeric",4),
                                   rep("numeric",4),
                                   rep("numeric",4))
)

modem <- modem %>% arrange(Time)
modem$HoursSinceUnixEpoch <- round(modem$Time/3600,digits = 0)
modem$TimePosix <- as.POSIXct(modem$Time,origin = "1970-01-01",tz="America/Chicago")

modem_transform <- modem %>% group_by(Time) %>% 
  mutate(Downstream.Power = mean(c(Downstream.Power.Level.A,
                                   Downstream.Power.Level.B,
                                   Downstream.Power.Level.C,
                                   Downstream.Power.Level.D),na.rm = TRUE)) %>%
  ungroup()
modem_transform <- modem_transform %>% group_by(Time) %>% 
  mutate(Downstream.Signal.to.Noise = mean(c(Downstream.Signal.to.Noise.A,
                                             Downstream.Signal.to.Noise.B,
                                             Downstream.Signal.to.Noise.C,
                                             Downstream.Signal.to.Noise.D),na.rm = TRUE)) %>%
  ungroup()

modem_transform <- modem_transform[,c("Time","Upstream.Power.Level.A","HoursSinceUnixEpoch","TimePosix","Downstream.Power","Downstream.Signal.to.Noise")]
# modem_transform_upstream <- modem %>% group_by(HoursSinceUnixEpoch) %>% 
#   summarize(Upstream.Power.Level = mean(Upstream.Power.Level,na.rm=TRUE)) %>%
#   ungroup()
# modem_transform <- full_join(modem_transform_downstream,
#                              modem_transform_upstream,
#                              by="HoursSinceUnixEpoch")

speeds_transform <- speeds[speeds$Ping..ms.<4000,
                           c("Time","Host.Location","Download.Speed..Mbits.s.","Upload.Speed..Mbits.s.","Ping..ms.")]
# speeds_transform <- speeds_transform %>% group_by(HoursSinceUnixEpoch,Host.Location) %>%
#   summarise_each(funs(mean(.,na.rm = TRUE))) %>%
#   ungroup()

speed.and.power <- full_join(modem_transform,speeds_transform,by="Time")

# ggpairs(speed.and.power,color="Host.Location")

# speeds_madison <- speeds_transform[speeds_transform$Host.Location=="Madison; WI" & complete.cases(speeds_transform),]
# speed.and.power.madison <- inner_join(modem_transform,speeds_madison,by="HoursSinceUnixEpoch")
speed.and.power.madison <- speed.and.power[complete.cases(speed.and.power),]
# speed.and.power.madison <- speed.and.power[speed.and.power$Host.Location=="Madison; WI" & complete.cases(speed.and.power),]
speed.and.power.madison <- speed.and.power.madison %>% mutate(Ping..ms. = log(Ping..ms.))
# speed.and.power.madison$PingTransition <- as.factor(speed.and.power.madison$HoursSinceUnixEpoch > 398158)
speed.and.power.madison$AttenuatorRemoved <- as.factor(speed.and.power.madison$Time > attenuator.removal & 
                                                       speed.and.power.madison$Time < attenuator.replace)
speed.and.power.madison$AmplifierRepaired <- as.factor(speed.and.power.madison$Time > amplifier.repair)
# speed.and.power.madison.recent <- speed.and.power.madison
speed.and.power.madison.recent <- speed.and.power.madison[speed.and.power.madison$Time > ((5+1)*amplifier.repair-5*now),]
# speed.and.power.madison.recent <- speed.and.power.madison[speed.and.power.madison$Time > (2*attenuator.removal-attenuator.replace),]
# speed.and.power.madison.recent$Time <- speed.and.power.madison.recent$TimePosix
# ggpairs(speed.and.power.madison.recent,
#         columns = c(1,2,5,7,8,9),
#         color="AttenuatorRemoved",
#         params=c(alpha=10/10,size=10/10))
g <- ggpairs(speed.and.power.madison.recent,
             mapping=aes(color=AmplifierRepaired),
             columns = c(1,2,5,7,8,9),
             upper = list(continuous = wrap("cor", size=3)),
             lower = list(continuous = wrap("density", alpha = 0.5)))
g

withAttenuator <- speed.and.power.madison.recent[speed.and.power.madison.recent$AttenuatorRemoved==FALSE,c("Download.Speed..Mbits.s.","Upload.Speed..Mbits.s.","Ping..ms.")]
withoutAttenuator <- speed.and.power.madison.recent[speed.and.power.madison.recent$AttenuatorRemoved==TRUE,c("Download.Speed..Mbits.s.","Upload.Speed..Mbits.s.","Ping..ms.")]
fixedAmplifier <- speed.and.power.madison.recent[speed.and.power.madison.recent$AmplifierRepaired==TRUE,c("Download.Speed..Mbits.s.","Upload.Speed..Mbits.s.","Ping..ms.")]
brokenAmplifier <- speed.and.power.madison.recent[speed.and.power.madison.recent$AmplifierRepaired==FALSE,c("Download.Speed..Mbits.s.","Upload.Speed..Mbits.s.","Ping..ms.")]

t.test(fixedAmplifier$Download.Speed..Mbits.s.,brokenAmplifier$Download.Speed..Mbits.s.)
t.test(fixedAmplifier$Upload.Speed..Mbits.s.,brokenAmplifier$Upload.Speed..Mbits.s.)
t.test(fixedAmplifier$Ping..ms.,brokenAmplifier$Ping..ms.)

# powerModel1 <- lm(Download.Speed..Mbits.s. ~ Downstream.Power,speed.and.power.madison)
# powerModel2 <- lm(Download.Speed..Mbits.s. ~ Downstream.Power + I(Downstream.Power^2),speed.and.power.madison)
# ggplot(data = speed.and.power.madison,
#        aes(x=Downstream.Power,y=Download.Speed..Mbits.s.)) + 
#   geom_point(alpha=0.3) +
#   stat_smooth(method="lm",col="blue")
# 
# speed.and.power.madison.recent <- speed.and.power.madison[speed.and.power.madison$Time > (1446422400),]

ggplot(data = speed.and.power.madison.recent,
       aes(x=TimePosix,y=Upstream.Power.Level.A,color=AmplifierRepaired)) + 
  geom_point(alpha=0.3) +
  scale_y_continuous(limits = c(30, 75)) + 
  xlab("") +
  ylab("Upstream Power Level (dBmV)") +
  theme(axis.text=element_text(size=16),
        axis.title=element_text(size=24))

ggplot(data = speed.and.power.madison.recent,
       aes(x=TimePosix,y=Downstream.Signal.to.Noise,color=AmplifierRepaired)) + 
  geom_point(alpha=0.3) +
  scale_y_continuous(limits = c(25, 45)) + 
  xlab("") +
  ylab("Downstream Signal to Noise (dB)") +
  theme(axis.text=element_text(size=16),
        axis.title=element_text(size=24))

ggplot(data = speed.and.power.madison.recent,
       aes(x=TimePosix,y=Downstream.Power,color=AmplifierRepaired)) + 
  geom_point(alpha=0.3) +
  scale_y_continuous(limits = c(0, 15)) + 
  xlab("") +
  ylab("Downstream Power Level (dBmV)") +
  theme(axis.text=element_text(size=16),
        axis.title=element_text(size=24))

#        aes(x=Downstream.Power,y=Upstream.Power.Level)) + 
#   geom_point(alpha=0.3) +
#   stat_smooth(method="lm",col="blue")
# 
# powerModel2 <- lm(Download.Speed..Mbits.s. ~ Downstream.Power + 
#                     Upstream.Power.Level + 
#                     Ping..ms.,
#                   speed.and.power.madison)
# summary(powerModel2)
