FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
WORKDIR /src
COPY Directory.Build.props Directory.Packages.props ./
COPY apps/shared/OrderFlow.Shared.csproj apps/shared/
COPY apps/worker/OrderFlow.Worker.csproj apps/worker/
RUN dotnet restore apps/worker/OrderFlow.Worker.csproj
COPY apps/shared/ apps/shared/
COPY apps/worker/ apps/worker/
RUN dotnet publish apps/worker/OrderFlow.Worker.csproj -c Release -o /app --no-restore /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0-alpine
WORKDIR /app
RUN apk add --no-cache krb5-libs
ENV ASPNETCORE_HTTP_PORTS=3001 \
    DOTNET_EnableDiagnostics=1
COPY --from=build /app ./
USER $APP_UID
EXPOSE 3001
ENTRYPOINT ["dotnet", "OrderFlow.Worker.dll"]
